package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.secret.Secret;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/**
 * MQTT 소스 설정을 읽어 검증한 값(DSC domain-model §2.2 MQTT_SUBSCRIBE, connectors.md §3, DSC-01.04·01.05·09.04).
 *
 * <p>설정 JSON은 core-api가 저장 때 커넥터 스키마로 검증한다(BR-DSC-22). 여기서는 연결에 필요한 값만 읽고, 빠진 값은 기본값을 쓴다.
 * 두 모양을 모두 읽는다: DSC 도메인 모델의 평평한 모양({@code url, qos, keepaliveSec, cleanStart, auth:"HEADER", headerName})과
 * connectors.md 예시 모양({@code version, subscriptions[], session{}, auth{type:"ws-header", scheme}}).
 *
 * @param version          {@code 5.0} 또는 {@code 3.1.1}
 * @param transport        tcp, ssl, ws, wss
 * @param host             브로커 호스트
 * @param port             브로커 포트
 * @param path             WebSocket 경로(ws·wss). 앞의 {@code /}는 뺀다
 * @param topics           구독 토픽과 QoS(공유 구독이면 {@code $share/{group}/}이 붙은 실제 필터)
 * @param keepAliveSec     keepalive(10~600)
 * @param cleanStart       true면 세션을 남기지 않는다. 기본 false(영속 세션, 무손실)
 * @param sessionExpirySec 영속 세션 만료(MQTT 5)
 * @param sharedGroup      MQTT 5 공유 구독 그룹. 없으면 null(두 인스턴스가 모두 받는 DUAL_ACTIVE)
 * @param retainHandling   MQTT 5 유지 메시지 처리: SEND, SEND_IF_SUBSCRIPTION_DOES_NOT_EXIST, DO_NOT_SEND
 * @param auth             NONE, USERPASS, HEADER, MTLS
 * @param username         USERPASS 사용자 이름
 * @param password         USERPASS 비밀번호. 없으면 null
 * @param headers          HEADER 인증의 WebSocket HTTP 헤더(이름 → 값). iot-data.java21.net은 {@code Authorization: Basic …}
 * @param tlsInsecure      인증서 검증 끄기(개발 소스만, BR-DSC-29)
 * @param caPem            추가로 믿을 CA(PEM). 없으면 JDK 기본 신뢰 저장소
 * @param clientCertPem    mTLS 클라이언트 인증서(PEM)
 * @param clientKeyPem     mTLS 개인 키(PKCS#8 PEM)
 */
public record MqttSourceSettings(String version, String transport, String host, int port, String path,
                                 List<Subscription> topics, int keepAliveSec, boolean cleanStart, long sessionExpirySec,
                                 String sharedGroup, String retainHandling, String auth, String username,
                                 Secret password, Map<String, String> headers, boolean tlsInsecure, String caPem,
                                 String clientCertPem, String clientKeyPem) {

    public static final String V5 = "5.0";
    public static final String V311 = "3.1.1";

    /** @param qos 0, 1, 2 */
    public record Subscription(String topic, int qos) {
    }

    public boolean v5() {
        return V5.equals(version);
    }

    public boolean webSocket() {
        return "ws".equals(transport) || "wss".equals(transport);
    }

    public boolean tls() {
        return "ssl".equals(transport) || "wss".equals(transport);
    }

    /** BR-DSC-25: MQTT 5 + 공유 구독이면 SCALABLE, 아니면 DUAL_ACTIVE */
    public ScalingMode scaling() {
        return v5() && sharedGroup != null ? ScalingMode.SCALABLE : ScalingMode.DUAL_ACTIVE;
    }

    /** 확인 없는 QoS 0 구독이 있으면 유실 가능(DSC-09.03, AT-DSC-20.4) */
    public boolean lossPossible() {
        return topics.stream().anyMatch(t -> t.qos() == 0) || cleanStart;
    }

    @Override
    public String toString() {
        return "MqttSourceSettings[" + version + " " + transport + "://" + host + ":" + port + "/" + path
                + ", topics=" + topics + ", auth=" + auth + ", shared=" + sharedGroup + "]";
    }

    /**
     * @throws InvalidSettingsException 필수 값이 없거나 형식이 틀리면(SOURCE_CONFIG_INVALID·SOURCE_SECRET_REQUIRED·SOURCE_AUTH_UNSUPPORTED)
     */
    public static MqttSourceSettings from(SourceConfig source) {
        JsonNode c = source.config();
        String url = text(c, "url", text(c, "brokerUrl", null));
        if (url == null) {
            throw InvalidSettingsException.config("url");
        }
        URI uri;
        try {
            uri = new URI(url.trim());
        } catch (URISyntaxException e) {
            throw InvalidSettingsException.config("url");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        String transport = switch (scheme) {
            case "tcp", "mqtt" -> "tcp";
            case "ssl", "mqtts", "tls" -> "ssl";
            case "ws" -> "ws";
            case "wss" -> "wss";
            default -> throw InvalidSettingsException.config("url");
        };
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw InvalidSettingsException.config("url");
        }
        int port = uri.getPort() > 0 ? uri.getPort() : switch (transport) {
            case "tcp" -> 1883;
            case "ssl" -> 8883;
            case "ws" -> 80;
            default -> 443;
        };
        String path = uri.getPath() == null || uri.getPath().isBlank() ? "mqtt" : uri.getPath().replaceFirst("^/", "");

        String version = text(c, "version", text(c, "protocolVersion", V5));
        version = switch (version) {
            case "5", "5.0", "MQTT_5", "MQTT5" -> V5;
            case "3.1.1", "3", "MQTT_3_1_1", "MQTT311" -> V311;
            default -> throw InvalidSettingsException.config("version");
        };

        JsonNode session = c.path("session");
        int keepAlive = intValue(c, "keepaliveSec", intValue(c, "keepAliveSec", intValue(session, "keepAliveSec", 60)));
        if (keepAlive < 10 || keepAlive > 600) {
            throw InvalidSettingsException.config("keepaliveSec");
        }
        boolean cleanStart = bool(c, "cleanStart", bool(c, "cleanSession", bool(session, "cleanStart", false)));
        long expiry = intValue(c, "sessionExpirySec", intValue(session, "sessionExpirySec", 3600));
        String shared = text(c, "sharedGroup", null);
        int defaultQos = intValue(c, "qos", 1);
        List<Subscription> subs = new ArrayList<>();
        for (String field : new String[]{"topics", "subscriptions"}) {
            for (JsonNode t : c.path(field)) {
                String topic = t.isString() ? t.asString() : text(t, "topic", null);
                if (topic == null || topic.isBlank()) {
                    throw InvalidSettingsException.config(field);
                }
                int qos = t.isObject() ? intValue(t, "qos", defaultQos) : defaultQos;
                if (shared == null && t.isObject() && t.path("shared").asBoolean(false)) {
                    shared = text(c, "clientIdBase", "data2flow-ingress");
                }
                subs.add(new Subscription(topic, qos));
            }
        }
        if (subs.isEmpty()) {
            throw InvalidSettingsException.config("topics");
        }
        if (bool(c, "downlinkAck", false)) {
            subs = withDownlinkAck(subs);
        }
        if (subs.size() > 20) {
            throw InvalidSettingsException.config("topics");
        }
        for (Subscription s : subs) {
            if (s.qos() < 0 || s.qos() > 2) {
                throw InvalidSettingsException.config("qos");
            }
        }
        if (shared != null) {
            if (!V5.equals(version) || !shared.matches("[A-Za-z0-9_-]{1,64}")) {
                throw InvalidSettingsException.config("sharedGroup");
            }
            String group = shared;
            subs = subs.stream().map(s -> new Subscription("$share/" + group + "/" + s.topic(), s.qos())).toList();
        }
        String retainHandling = text(c, "retainHandling", "SEND");

        JsonNode authNode = c.path("auth");
        String auth;
        String scheme2 = null;
        String headerName;
        if (authNode.isObject()) {
            String type = text(authNode, "type", "none").toLowerCase(Locale.ROOT);
            auth = switch (type) {
                case "none" -> "NONE";
                case "basic", "userpass", "user-password", "user_password" -> "USERPASS";
                case "ws-header", "header" -> "HEADER";
                case "mtls" -> "MTLS";
                default -> throw new InvalidSettingsException(InvalidSettingsException.Reason.AUTH_UNSUPPORTED, "auth");
            };
            scheme2 = text(authNode, "scheme", null);
            headerName = text(authNode, "headerName", "Authorization");
        } else {
            auth = text(c, "auth", "NONE").toUpperCase(Locale.ROOT);
            headerName = text(c, "headerName", "Authorization");
            scheme2 = text(c, "headerScheme", null);
        }
        String username = text(c, "username", null);
        Secret password = null;
        Map<String, String> headers = Map.of();
        switch (auth) {
            case "NONE" -> {
            }
            case "USERPASS" -> {
                password = secret(source, "PASSWORD");
                if (password == null) {
                    throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "PASSWORD");
                }
                if (username == null) {
                    throw InvalidSettingsException.config("username");
                }
            }
            case "HEADER" -> {
                if (!"ws".equals(transport) && !"wss".equals(transport)) {
                    throw new InvalidSettingsException(InvalidSettingsException.Reason.AUTH_UNSUPPORTED, "auth");
                }
                Secret value = secret(source, "HEADER_VALUE");
                if (value == null) {
                    throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "HEADER_VALUE");
                }
                headers = Map.of(headerName, headerValue(scheme2, headerName, value.reveal()));
            }
            case "MTLS" -> {
                if (secret(source, "CLIENT_CERT") == null || secret(source, "CLIENT_KEY") == null) {
                    throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "CLIENT_CERT");
                }
            }
            default -> throw new InvalidSettingsException(InvalidSettingsException.Reason.AUTH_UNSUPPORTED, "auth");
        }
        JsonNode tls = c.path("tls");
        boolean insecure = bool(c, "tlsInsecure", tls.isObject() && !tls.path("verify").asBoolean(true));
        return new MqttSourceSettings(version, transport, uri.getHost(), port, path, List.copyOf(subs), keepAlive,
                cleanStart, expiry, shared, retainHandling, auth, username, password, headers, insecure,
                reveal(secret(source, "CA_CERT")), reveal(secret(source, "CLIENT_CERT")),
                reveal(secret(source, "CLIENT_KEY")));
    }

    /** ChirpStack v4 업링크 토픽 {@code application/{appId}/device/{devEui}/event/up}(와일드카드 포함) */
    private static final java.util.regex.Pattern CHIRPSTACK_UP =
            java.util.regex.Pattern.compile("^(application/[^/]+/device/[^/]+/event/)up$");

    /**
     * {@code downlinkAck=true}(ACT-03.03, ADR-054 남은 것 ①): ChirpStack v4 업링크 토픽마다 같은 애플리케이션·기기 범위의
     * {@code event/ack}(확인형 다운링크의 기기 확인)와 {@code event/txack}(게이트웨이 송신)을 QoS 1 이상으로 더 구독한다. 구독만 하고 발행하지
     * 않는다(CLAUDE.md §5). 받은 ack는 ingress가 원본 스트림이 아니라 EVT-ACT-09 {@code lorawan.downlink.ack}로 낸다.
     *
     * @throws InvalidSettingsException ChirpStack 업링크 토픽이 하나도 없으면({@code downlinkAck})
     */
    static List<Subscription> withDownlinkAck(List<Subscription> subs) {
        List<Subscription> out = new ArrayList<>(subs);
        boolean found = false;
        for (Subscription s : subs) {
            java.util.regex.Matcher m = CHIRPSTACK_UP.matcher(s.topic());
            if (!m.matches()) {
                continue;
            }
            found = true;
            int qos = Math.max(1, s.qos());
            for (String event : new String[]{"ack", "txack"}) {
                Subscription extra = new Subscription(m.group(1) + event, qos);
                if (out.stream().noneMatch(o -> o.topic().equals(extra.topic()))) {
                    out.add(extra);
                }
            }
        }
        if (!found) {
            throw InvalidSettingsException.config("downlinkAck");
        }
        return out;
    }

    /**
     * HEADER 값. Basic이고 값이 {@code 사용자:비밀번호} 모양이면 Base64로 바꿔 {@code Basic …}을 만든다(.env {@code MQTT_BASIC_AUTH} 형식).
     */
    static String headerValue(String scheme, String headerName, String raw) {
        String value = raw.trim();
        boolean basic = "basic".equalsIgnoreCase(scheme)
                || (scheme == null && "authorization".equalsIgnoreCase(headerName) && value.contains(":") && !value.contains(" "));
        if (basic && !value.regionMatches(true, 0, "Basic ", 0, 6)) {
            return "Basic " + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }
        if (scheme != null && !basic && !value.regionMatches(true, 0, scheme + " ", 0, scheme.length() + 1)) {
            return scheme + " " + value;
        }
        return value;
    }

    private static Secret secret(SourceConfig source, String kind) {
        for (Map.Entry<String, Secret> e : source.secrets().entrySet()) {
            if (e.getKey().equalsIgnoreCase(kind) || e.getKey().replace("_", "").equalsIgnoreCase(kind.replace("_", ""))) {
                return e.getValue();
            }
        }
        return null;
    }

    private static String reveal(Secret secret) {
        return secret == null ? null : secret.reveal();
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || v.isNull() || !v.isValueNode() || v.asString("").isBlank() ? fallback : v.asString();
    }

    private static int intValue(JsonNode node, String field, int fallback) {
        JsonNode v = node == null ? null : node.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        if (!v.isNumber() && !(v.isString() && v.asString().matches("-?\\d+"))) {
            throw InvalidSettingsException.config(field);
        }
        return v.asInt();
    }

    private static boolean bool(JsonNode node, String field, boolean fallback) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || v.isNull() ? fallback : v.asBoolean(fallback);
    }
}
