package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MQTT 프리셋 커넥터 4종(DSC-09 카탈로그, connectors.md §2·§4 템플릿). 클라우드 서비스(The Things Stack, AWS IoT, Azure IoT Hub)는 실제
 * 계정이 없어 구현과 계약 시험(Mosquitto 상대)까지만 했다(ADR-040). 실제 서비스의 인증 방식(AWS ALPN 443, Azure SAS 갱신)은 계정이
 * 생기면 확인한다.
 */
public final class MqttPresets {

    public static final String SPARKPLUG = "sparkplug-b";
    public static final String TTS = "tts-v3";
    public static final String AWS_IOT = "aws-iot-core";
    public static final String AZURE_IOT_HUB = "azure-iot-hub";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Set<PayloadFormat> JSON_FORMATS = Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.BINARY);

    private MqttPresets() {
    }

    public static List<MqttPresetConnector> all(MqttSourceConnector mqtt, Clock clock) {
        return List.of(sparkplug(mqtt), theThingsStack(mqtt), awsIot(mqtt), azureIotHub(mqtt, clock));
    }

    /**
     * Sparkplug B(Eclipse Sparkplug 3.0): {@code spBv1.0/{groupId}/#}를 구독하고 NBIRTH·NDATA·NDEATH·DBIRTH·DDATA·DDEATH만 기록한다.
     * STATE(호스트 앱 상태)와 NCMD·DCMD(명령)는 기록하지 않는다. payload(Protobuf)는 그대로 넘기고 pipeline이 해석한다(형식 SPARKPLUG_B).
     * 호스트 앱으로 STATE를 발행하거나 재탄생(Rebirth) 명령을 보내지 않는다(구독 전용, CLAUDE.md §5).
     */
    public static MqttPresetConnector sparkplug(MqttSourceConnector mqtt) {
        return new MqttPresetConnector(SPARKPLUG, "Sparkplug B", ConnectorCategory.MQTT,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.MTLS), Set.of(PayloadFormat.SPARKPLUG_B), mqtt,
                preset -> {
                    Cfg c = Cfg.of(preset);
                    String group = c.text("groupId", "+");
                    if (!group.equals("+") && !group.matches("[^/#+]{1,64}")) {
                        throw InvalidSettingsException.config("groupId");
                    }
                    ObjectNode m = base(c, c.required("url"));
                    topics(m, "spBv1.0/" + group + "/#");
                    return mqttConfig(preset, m, preset.secrets());
                }, MqttPresets::sparkplugData);
    }

    /** Sparkplug 데이터 토픽인가: {@code spBv1.0/{group}/{N|D}{BIRTH|DATA|DEATH}/…} */
    static boolean sparkplugData(String topic) {
        String[] p = topic == null ? new String[0] : topic.split("/");
        return p.length >= 4 && "spBv1.0".equals(p[0]) && !"STATE".equals(p[1])
                && Set.of("NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH").contains(p[2]);
    }

    /**
     * The Things Stack v3 MQTT 통합: 사용자 이름 {@code {applicationId}@{tenantId}}, 비밀번호는 API 키(비밀값 PASSWORD),
     * 토픽 {@code v3/{사용자 이름}/devices/+/up}, 디코더 {@code tts-v3}(pipeline).
     */
    public static MqttPresetConnector theThingsStack(MqttSourceConnector mqtt) {
        return new MqttPresetConnector(TTS, "The Things Stack v3", ConnectorCategory.LORAWAN,
                Set.of(AuthMethod.USER_PASSWORD), JSON_FORMATS, mqtt, preset -> {
                    Cfg c = Cfg.of(preset);
                    String app = c.required("applicationId");
                    String tenant = c.text("tenantId", "ttn");
                    if (!app.matches("[a-z0-9](?:[-]?[a-z0-9]){1,35}") || !tenant.matches("[a-z0-9](?:[-]?[a-z0-9]){1,35}")) {
                        throw InvalidSettingsException.config("applicationId");
                    }
                    String url = c.text("url", "ssl://" + c.required("host") + ":8883");
                    ObjectNode m = base(c, url);
                    String username = app + "@" + tenant;
                    m.put("auth", "USERPASS").put("username", username);
                    topics(m, "v3/" + username + "/devices/+/" + c.text("event", "up"));
                    return mqttConfig(preset, m, preset.secrets());
                }, t -> true);
    }

    /**
     * AWS IoT Core: 엔드포인트 {@code {id}-ats.iot.{region}.amazonaws.com:8883}에 X.509 클라이언트 인증서(mTLS)로 접속하고 지정 토픽을
     * 구독한다. 비밀값 CLIENT_CERT·CLIENT_KEY·CA_CERT(Amazon Root CA). ALPN 443({@code x-amzn-mqtt-ca})과 사용자 정의 인증은 아직 없다.
     */
    public static MqttPresetConnector awsIot(MqttSourceConnector mqtt) {
        return new MqttPresetConnector(AWS_IOT, "AWS IoT Core", ConnectorCategory.CLOUD_HUB, Set.of(AuthMethod.MTLS),
                JSON_FORMATS, mqtt, preset -> {
                    Cfg c = Cfg.of(preset);
                    String url = c.text("url", "ssl://" + c.required("endpoint") + ":8883");
                    ObjectNode m = base(c, url);
                    m.put("auth", "MTLS");
                    List<String> topics = c.strings("topics");
                    if (topics.isEmpty()) {
                        throw InvalidSettingsException.config("topics");
                    }
                    topics(m, topics.toArray(String[]::new));
                    return mqttConfig(preset, m, preset.secrets());
                }, t -> true);
    }

    /**
     * Azure IoT Hub(MQTT 3.1.1 장치 엔드포인트): 사용자 이름 {@code {hub}.azure-devices.net/{deviceId}/?api-version=2021-04-12}, 비밀번호는
     * 비밀값 SAS_KEY(장치 대칭 키, Base64)로 만든 SAS 토큰(기본 24시간). 장치가 구독할 수 있는 토픽은 클라우드→장치
     * {@code devices/{deviceId}/messages/devicebound/#}뿐이다. 장치→클라우드 텔레메트리 전체를 받으려면 Event Hubs 호환 엔드포인트(AMQP 1.0,
     * {@code amqp10} 커넥터)를 쓴다. SAS 토큰이 만료되기 전에 다시 접속해야 하므로 토큰 수명을 연결 수명보다 길게 둔다.
     */
    public static MqttPresetConnector azureIotHub(MqttSourceConnector mqtt, Clock clock) {
        return new MqttPresetConnector(AZURE_IOT_HUB, "Azure IoT Hub (MQTT)", ConnectorCategory.CLOUD_HUB,
                Set.of(AuthMethod.TOKEN), JSON_FORMATS, mqtt, preset -> {
                    Cfg c = Cfg.of(preset);
                    String hub = c.required("hubName");
                    String device = c.required("deviceId");
                    if (!hub.matches("[A-Za-z0-9-]{3,50}") || !device.matches("[A-Za-z0-9\\-.%_*?!(),:=@$']{1,128}")) {
                        throw InvalidSettingsException.config("hubName");
                    }
                    String host = hub + ".azure-devices.net";
                    ObjectNode m = base(c, c.text("url", "ssl://" + host + ":8883"));
                    m.put("version", "3.1.1");
                    m.put("auth", "USERPASS").put("username", host + "/" + device + "/?api-version=2021-04-12");
                    List<String> topics = c.strings("topics");
                    topics(m, topics.isEmpty() ? new String[]{"devices/" + device + "/messages/devicebound/#"}
                            : topics.toArray(String[]::new));
                    Duration ttl = c.seconds("sasTtlSec", 86_400, 300, 30L * 86_400);
                    String token = sasToken(host + "/devices/" + device, c.requiredSecret("SAS_KEY").reveal(),
                            clock.instant().plus(ttl).getEpochSecond());
                    Map<String, Secret> secrets = new HashMap<>(preset.secrets());
                    secrets.put("PASSWORD", Secret.of(token));
                    return mqttConfig(preset, m, secrets);
                }, t -> true);
    }

    /** Azure SAS 토큰: {@code SharedAccessSignature sr={uri}&sig={HMAC-SHA256(key, uri + "\n" + expiry)}&se={expiry}} */
    public static String sasToken(String resourceUri, String base64Key, long expiryEpochSec) {
        try {
            String sr = URLEncoder.encode(resourceUri, StandardCharsets.UTF_8);
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(java.util.Base64.getDecoder().decode(base64Key), "HmacSHA256"));
            String sig = java.util.Base64.getEncoder().encodeToString(
                    mac.doFinal((sr + "\n" + expiryEpochSec).getBytes(StandardCharsets.UTF_8)));
            return "SharedAccessSignature sr=" + sr + "&sig=" + URLEncoder.encode(sig, StandardCharsets.UTF_8) + "&se="
                    + expiryEpochSec;
        } catch (IllegalArgumentException e) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "SAS_KEY");
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 프리셋 설정에서 MQTT 공통 값(버전·세션·공유 구독·TLS)을 옮긴다 */
    private static ObjectNode base(Cfg c, String url) {
        ObjectNode m = JSON.createObjectNode();
        m.put("url", url);
        m.put("version", c.text("version", "3.1.1"));
        m.put("keepaliveSec", c.integer("keepaliveSec", 60, 10, 600));
        m.put("cleanStart", false);
        m.put("sessionExpirySec", c.integer("sessionExpirySec", 3600, 0, 604_800));
        m.put("qos", c.integer("qos", 1, 0, 2));
        String shared = c.text("sharedGroup", null);
        if (shared != null) {
            m.put("sharedGroup", shared);
        }
        String clientIdBase = c.text("clientIdBase", null);
        if (clientIdBase != null) {
            m.put("clientIdBase", clientIdBase);
        }
        if (c.bool("tlsInsecure", false)) {
            m.put("tlsInsecure", true);
        }
        String user = c.text("username", null);
        if (user != null) {
            m.put("auth", "USERPASS").put("username", user);
        }
        String auth = c.text("auth", null);
        if ("MTLS".equals(auth)) {
            m.put("auth", "MTLS");
        }
        return m;
    }

    private static void topics(ObjectNode m, String... topics) {
        ArrayNode a = m.putArray("topics");
        for (String t : topics) {
            a.addObject().put("topic", t).put("qos", m.path("qos").asInt(1));
        }
    }

    private static SourceConfig mqttConfig(SourceConfig preset, JsonNode mqttConfig, Map<String, Secret> secrets) {
        return new SourceConfig(preset.organizationId(), preset.sourceId(), preset.sourceType(), preset.connectorKey(),
                mqttConfig, secrets, preset.clientId());
    }
}
