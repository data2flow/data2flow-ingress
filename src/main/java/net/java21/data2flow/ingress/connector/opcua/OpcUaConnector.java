package net.java21.data2flow.ingress.connector.opcua;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.ConnectorTls;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.common.WriteFailedException;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.CertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.util.EndpointUtil;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * OPC UA 구독 커넥터(키 {@code opcua}, 소스 유형 OPCUA, DSC-05.01, IEC 62541, connectors.md §2 Eclipse Milo EPL-2.0 조건부). 노드마다
 * 모니터링 항목을 만들고 값이 바뀔 때 받은 DataValue 하나를 원본 하나로 넘긴다(AT-DSC-11.2: 값이 바뀔 때만).
 *
 * <ul>
 *   <li>payload {@code json}(기본): {@code {"nodeId","value","sourceTime","serverTime","status"}}. {@code value}: 문자열·바이트 값을 그대로.</li>
 *   <li>보안 정책 None·Basic256Sha256(Sign·SignAndEncrypt, 클라이언트 인증서 CLIENT_CERT·CLIENT_KEY, 서버 인증서는 CA_CERT로 검증), 사용자 인증
 *       익명·사용자/비밀번호(PASSWORD).</li>
 *   <li>구독 알림은 상대가 다시 보내 주지 않으므로 확인 방식 NONE(유실 가능). 끊기면 Milo가 다시 접속하고, 다시 만들 수 없으면 세션을 새로 맺는다.
 *       세션 하나라 리더 1대만 실행한다(SINGLETON). <b>쓰기(Write)는 하지 않는다</b>(송신은 DSC-09.13, M7).</li>
 * </ul>
 */
public class OpcUaConnector implements SourceConnector {

    public static final String KEY = "opcua";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final PollingOptions options;
    private final Clock clock;

    public OpcUaConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "OPC UA 구독", "1.0.0", ConnectorCategory.INDUSTRIAL,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.MTLS), Set.of(PayloadFormat.JSON, PayloadFormat.TEXT),
                AckMode.NONE, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        URI uri = URI.create(s.endpointUrl.replaceFirst("^opc\\.tcp", "tcp"));
        return new StagedConnectionTest<Client>(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 4840, false, null, true,
                options.testTimeout(), ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> new Client(connect(s, config, "data2flow-ingress-test")), (c, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    List<DataValue> values = c.client.readValues(0, TimestampsToReturn.Both,
                            s.nodes.stream().map(Node::id).toList());
                    for (int i = 0; i < values.size(); i++) {
                        StatusCode sc = values.get(i).getStatusCode();
                        if (sc != null && sc.isBad()) {
                            throw new IOException("노드를 읽을 수 없습니다: " + s.nodes.get(i).raw() + " " + sc);
                        }
                        if (preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                            preview.add(StagedConnectionTest.preview(clock, s.nodes.get(i).topic(),
                                    payload(s, s.nodes.get(i), values.get(i))));
                        }
                    }
                    return preview;
                }, true);
    }

    private record Client(OpcUaClient client) implements AutoCloseable {
        @Override
        public void close() {
            try {
                client.disconnect();
            } catch (UaException ignored) {
                // 닫는 중 오류는 무시
            }
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    /** 노드 하나: 원래 표기, NodeId, 주제(이름) */
    record Node(String raw, NodeId id, String topic) {
    }

    record Settings(String endpointUrl, SecurityPolicy policy, MessageSecurityMode mode, String username,
                    List<Node> nodes, double samplingMs, double publishingMs, int queueSize, boolean rawValue) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            String url = c.required("endpointUrl");
            if (!url.matches("opc\\.tcp://[^\\s/:]+(:\\d{1,5})?(/\\S*)?")) {
                throw InvalidSettingsException.config("endpointUrl");
            }
            SecurityPolicy policy = switch (c.text("securityPolicy", "None")) {
                case "None" -> SecurityPolicy.None;
                case "Basic256Sha256" -> SecurityPolicy.Basic256Sha256;
                default -> throw InvalidSettingsException.config("securityPolicy");
            };
            MessageSecurityMode mode = policy == SecurityPolicy.None ? MessageSecurityMode.None
                    : switch (c.text("securityMode", "SignAndEncrypt")) {
                case "Sign" -> MessageSecurityMode.Sign;
                case "SignAndEncrypt" -> MessageSecurityMode.SignAndEncrypt;
                default -> throw InvalidSettingsException.config("securityMode");
            };
            if (policy != SecurityPolicy.None) {
                c.requiredSecret("CLIENT_CERT");
                c.requiredSecret("CLIENT_KEY");
            }
            List<Node> nodes = new ArrayList<>();
            for (JsonNode n : c.node().path("nodes")) {
                String raw = n.isString() ? n.asString() : n.path("nodeId").asString("");
                NodeId id = NodeId.parseOrNull(raw);
                if (id == null) {
                    throw InvalidSettingsException.config("nodes");
                }
                String name = n.isObject() ? n.path("name").asString(raw) : raw;
                nodes.add(new Node(raw, id, name.isBlank() ? raw : name));
            }
            if (nodes.isEmpty() || nodes.size() > 1000) {
                throw InvalidSettingsException.config("nodes");
            }
            String format = c.text("payload", "json").toLowerCase(Locale.ROOT);
            if (!Set.of("json", "value").contains(format)) {
                throw InvalidSettingsException.config("payload");
            }
            return new Settings(url, policy, mode, c.text("username", null), List.copyOf(nodes),
                    c.decimal("samplingIntervalMs", 0), c.decimal("publishingIntervalMs", 500),
                    c.integer("queueSize", 100, 1, 10_000), "value".equals(format));
        }
    }

    OpcUaClient connect(Settings s, SourceConfig config, String sessionName) throws Exception {
        Cfg cfg = Cfg.of(config);
        String host = EndpointUtil.getHost(s.endpointUrl);
        int port = EndpointUtil.getPort(s.endpointUrl);
        ConnectorTls tls = ConnectorTls.from(cfg);
        OpcUaClient client = OpcUaClient.create(s.endpointUrl,
                endpoints -> endpoints.stream()
                        .filter(e -> s.policy.getUri().equals(e.getSecurityPolicyUri()) && e.getSecurityMode() == s.mode)
                        .findFirst()
                        .map(e -> EndpointUtil.updateUrl(e, host, port)),   // 서버가 알려 주는 주소 대신 설정한 주소로 접속
                transport -> { },
                b -> {
                    b.setApplicationName(LocalizedText.english("data2flow-ingress"))
                            .setApplicationUri("urn:java21:data2flow:ingress")
                            .setSessionName(() -> sessionName)
                            .setRequestTimeout(UInteger.valueOf(10_000))
                            .setIdentityProvider(s.username == null ? new AnonymousProvider()
                                    : new UsernameProvider(s.username, cfg.requiredSecret("PASSWORD").reveal()));
                    if (s.policy != SecurityPolicy.None) {
                        try {
                            X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(
                                    new java.io.ByteArrayInputStream(tls.certPem().getBytes(StandardCharsets.US_ASCII)));
                            b.setCertificate(cert).setKeyPair(new KeyPair(cert.getPublicKey(), privateKey(tls.keyPem())))
                                    .setCertificateValidator(validator(tls));
                        } catch (Exception e) {
                            throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "CLIENT_CERT");
                        }
                    }
                });
        client.connect();
        return client;
    }

    private static java.security.PrivateKey privateKey(String pem) throws Exception {
        byte[] der = java.util.Base64.getDecoder().decode(
                pem.replaceAll("-----(BEGIN|END) [A-Z ]*PRIVATE KEY-----", "").replaceAll("\\s", ""));
        return java.security.KeyFactory.getInstance("RSA").generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(der));
    }

    /** 서버 인증서 검증: CA_CERT로 PKIX(없거나 검증 끄기면 받아들임 — 개발 소스만, BR-DSC-29) */
    static CertificateValidator validator(ConnectorTls tls) {
        return (chain, applicationUri, hostnames) -> {
            if (tls.insecure() || tls.caPem() == null) {
                return;
            }
            try {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                Set<TrustAnchor> anchors = new HashSet<>();
                for (var ca : cf.generateCertificates(new java.io.ByteArrayInputStream(tls.caPem().getBytes(StandardCharsets.US_ASCII)))) {
                    anchors.add(new TrustAnchor((X509Certificate) ca, null));
                }
                PKIXParameters params = new PKIXParameters(anchors);
                params.setRevocationEnabled(false);
                CertPathValidator.getInstance("PKIX").validate(cf.generateCertPath(chain), params);
            } catch (Exception e) {
                throw new UaException(StatusCode.BAD.getValue(), "서버 인증서를 믿을 수 없습니다: " + e.getMessage());
            }
        };
    }

    /** DataValue → payload */
    static byte[] payload(Settings s, Node node, DataValue dv) {
        Object value = dv.getValue() == null ? null : dv.getValue().getValue();
        if (s.rawValue) {
            if (value instanceof String str) {
                return str.getBytes(StandardCharsets.UTF_8);
            }
            if (value instanceof ByteString bs) {
                return bs.bytesOrEmpty();
            }
            return JSON.writeValueAsBytes(jsonValue(value));
        }
        ObjectNode o = JSON.createObjectNode();
        o.put("nodeId", node.raw());
        o.set("value", JSON.valueToTree(jsonValue(value)));
        Optional.ofNullable(dv.getSourceTime()).map(DateTime::getJavaInstant).ifPresent(t -> o.put("sourceTime", t.toString()));
        Optional.ofNullable(dv.getServerTime()).map(DateTime::getJavaInstant).ifPresent(t -> o.put("serverTime", t.toString()));
        StatusCode sc = dv.getStatusCode();
        o.put("status", sc == null || sc.isGood() ? "GOOD" : sc.isUncertain() ? "UNCERTAIN" : "BAD");
        return JSON.writeValueAsBytes(o);
    }

    private static Object jsonValue(Object value) {
        if (value == null || value instanceof Number || value instanceof Boolean || value instanceof String) {
            return value;
        }
        if (value instanceof org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UNumber u) {
            return u.longValue();
        }
        if (value instanceof DateTime dt) {
            return dt.getJavaInstant().toString();
        }
        if (value instanceof ByteString bs) {
            return java.util.Base64.getEncoder().encodeToString(bs.bytesOrEmpty());
        }
        if (value instanceof Object[] arr) {
            List<Object> list = new ArrayList<>();
            for (Object e : arr) {
                list.add(jsonValue(e));
            }
            return list;
        }
        return String.valueOf(value);
    }

    private record Change(Node node, DataValue value) {
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private final BlockingDeque<Change> queue = new LinkedBlockingDeque<>(100_000);
        private OpcUaClient client;
        private OpcUaSubscription subscription;
        private volatile String failure;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            failure = null;
            client = OpcUaConnector.this.connect(settings, config,
                    config.clientId() == null ? "data2flow-ingress" : config.clientId());
            subscription = new OpcUaSubscription(client, settings.publishingMs);
            subscription.setSubscriptionListener(new OpcUaSubscription.SubscriptionListener() {
                @Override
                public void onTransferFailed(OpcUaSubscription s, StatusCode status) {
                    failure = "구독을 옮기지 못했습니다: " + status;
                }

                @Override
                public void onWatchdogTimerElapsed(OpcUaSubscription s) {
                    failure = "구독 응답이 없습니다(keep-alive 초과)";
                }

                @Override
                public void onNotificationDataLost(OpcUaSubscription s) {
                    // 서버 큐 넘침: 유실 가능(AckMode NONE). 계속 받는다
                }
            });
            subscription.create();
            List<OpcUaMonitoredItem> items = new ArrayList<>();
            for (Node n : settings.nodes) {
                OpcUaMonitoredItem item = OpcUaMonitoredItem.newDataItem(n.id());
                item.setSamplingInterval(settings.samplingMs);
                item.setQueueSize(UInteger.valueOf(settings.queueSize));
                item.setDiscardOldest(true);
                item.setDataValueListener((it, dv) -> {
                    if (!queue.offer(new Change(n, dv))) {
                        failure = "대기열이 가득 찼습니다";
                    }
                });
                items.add(item);
            }
            subscription.addMonitoredItems(items);
            subscription.synchronizeMonitoredItems();
            for (OpcUaMonitoredItem item : items) {
                StatusCode sc = item.getCreateResult().orElse(StatusCode.GOOD);
                if (sc.isBad()) {
                    throw new IOException("모니터링 항목을 만들 수 없습니다: " + item.getReadValueId().getNodeId() + " " + sc);
                }
            }
        }

        @Override
        protected boolean pollOnce() throws Exception {
            if (failure != null) {
                throw new IOException(failure);
            }
            Change first = queue.poll(Math.max(50, options.idleInterval().toMillis()), TimeUnit.MILLISECONDS);
            if (first == null) {
                return false;
            }
            List<Change> list = new ArrayList<>();
            list.add(first);
            queue.drainTo(list, 499);
            if (isPaused()) {
                for (int i = list.size() - 1; i >= 0; i--) {
                    queue.offerFirst(list.get(i));
                }
                return false;
            }
            List<RawEnvelope> batch = new ArrayList<>(list.size());
            for (Change ch : list) {
                byte[] payload = payload(settings, ch.node(), ch.value());
                batch.add(envelope(ch.node().topic(), payload));
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                for (int i = list.size() - 1; i >= 0; i--) {   // 아직 메모리에 있으니 되돌려 다시 기록한다
                    queue.offerFirst(list.get(i));
                }
                throw e;
            }
            return true;
        }

        @Override
        protected void disconnect() {
            try {
                if (subscription != null) {
                    subscription.delete();
                }
            } catch (Exception ignored) {
                // 끊긴 세션
            }
            try {
                if (client != null) {
                    client.disconnect();
                }
            } catch (Exception ignored) {
                // 닫는 중 오류는 무시
            }
            subscription = null;
            client = null;
        }
    }

}
