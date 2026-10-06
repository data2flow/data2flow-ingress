package net.java21.data2flow.ingress.connector.gcp;

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
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.RefreshingToken;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.common.WriteFailedException;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/**
 * Google Cloud Pub/Sub 구독 커넥터(키 {@code gcp-pubsub}). REST v1 {@code :pull} → 기록(confirm) → {@code :acknowledge}(AFTER_WRITE).
 * 기록이 실패하면 {@code :modifyAckDeadline 0}으로 바로 다시 받는다. 같은 구독을 여러 인스턴스가 나눠 받는다(SCALABLE).
 *
 * <p>connectors.md §2는 {@code google-cloud-pubsub}(Apache-2.0, gRPC)를 적었지만 의존성이 커서(gRPC·Guava·protobuf) JDK HttpClient로
 * REST를 직접 부른다(같은 의미, ADR-052). 인증은 서비스 계정 키(비밀값 {@code GCP_SERVICE_ACCOUNT}, JSON)로 받은 토큰이고
 * 만료 전에 갱신한다(BR-DSC-27). {@code endpoint}가 http면 에뮬레이터로 보고 인증하지 않는다. 실제 계정이 없으므로 계약 시험은 Pub/Sub
 * REST를 흉내 낸 서버로만 한다(ADR-040).
 */
public class PubSubConnector implements SourceConnector {

    public static final String KEY = "gcp-pubsub";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final PollingOptions options;
    private final Clock clock;
    private final HttpClient http;

    public PubSubConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(options.connectTimeout()).build();
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "Google Cloud Pub/Sub", "1.0.0", ConnectorCategory.CLOUD_HUB,
                Set.of(AuthMethod.OAUTH2_CC, AuthMethod.NONE),
                Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.BINARY, PayloadFormat.PROTOBUF,
                        PayloadFormat.AVRO),
                AckMode.AFTER_WRITE, ScalingMode.SCALABLE, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        Client client = new Client(s, config);
        int port = s.endpoint.getPort() > 0 ? s.endpoint.getPort() : "https".equals(s.endpoint.getScheme()) ? 443 : 80;
        return new StagedConnectionTest<Client>(s.endpoint.getHost(), port, false, null, true, options.testTimeout(), "SUBSCRIBE")
                .run(remaining -> {
                    client.token();
                    return client;
                }, (c, remaining) -> {
                    c.get("");   // 구독이 있는지(미리보기는 ack 기한을 바꾸지 않도록 하지 않는다)
                    return List.of();
                }, false);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    record Settings(URI endpoint, String project, String subscription, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            URI endpoint = URI.create(c.text("endpoint", "https://pubsub.googleapis.com"));
            if (endpoint.getHost() == null || !Set.of("http", "https").contains(endpoint.getScheme())) {
                throw InvalidSettingsException.config("endpoint");
            }
            String project = c.required("project");
            String subscription = c.required("subscription");
            if (!project.matches("[a-z][a-z0-9-]{4,62}") && !project.matches("[A-Za-z0-9-]{1,63}")) {
                throw InvalidSettingsException.config("project");
            }
            if (!subscription.matches("[A-Za-z][A-Za-z0-9._~+%-]{2,254}")) {
                throw InvalidSettingsException.config("subscription");
            }
            if ("https".equals(endpoint.getScheme()) && Cfg.of(config).secret("GCP_SERVICE_ACCOUNT") == null) {
                throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "GCP_SERVICE_ACCOUNT");
            }
            return new Settings(endpoint, project, subscription, c.integer("batchSize", 100, 1, 1000));
        }

        String base() {
            return endpoint.toString().replaceAll("/$", "") + "/v1/projects/" + project + "/subscriptions/" + subscription;
        }
    }

    /** Pub/Sub REST 호출(토큰 포함) */
    private final class Client implements AutoCloseable {
        private final Settings settings;
        private final RefreshingToken token;

        Client(Settings settings, SourceConfig config) {
            this.settings = settings;
            String sa = Cfg.of(config).reveal("GCP_SERVICE_ACCOUNT");
            if ("http".equals(settings.endpoint.getScheme()) || sa == null) {
                this.token = null;
            } else {
                try {
                    this.token = new RefreshingToken(new GoogleServiceAccount(sa, http, clock), clock);
                } catch (Exception e) {
                    throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "GCP_SERVICE_ACCOUNT");
                }
            }
        }

        String token() throws Exception {
            return token == null ? null : token.get();
        }

        JsonNode get(String suffix) throws Exception {
            return call(HttpRequest.newBuilder(URI.create(settings.base() + suffix)).GET());
        }

        JsonNode post(String action, JsonNode body) throws Exception {
            return call(HttpRequest.newBuilder(URI.create(settings.base() + ":" + action))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body))));
        }

        private JsonNode call(HttpRequest.Builder b) throws Exception {
            String t = token();
            if (t != null) {
                b.header("Authorization", "Bearer " + t);
            }
            HttpResponse<byte[]> res = http.send(b.timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofByteArray());
            if (res.statusCode() == 401 || res.statusCode() == 403) {
                if (token != null) {
                    token.invalidate();
                }
                throw new SecurityException("not authorized HTTP " + res.statusCode());
            }
            if (res.statusCode() / 100 != 2) {
                throw new IOException("Pub/Sub HTTP " + res.statusCode());
            }
            return res.body().length == 0 ? JSON.createObjectNode() : JSON.readTree(res.body());
        }

        @Override
        public void close() {
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Client client;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), options.idleInterval(), options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            client = new Client(settings, config);
            client.get("");
        }

        @Override
        protected boolean pollOnce() throws Exception {
            JsonNode res = client.post("pull", JSON.createObjectNode().put("maxMessages", settings.batchSize));
            List<String> ackIds = new ArrayList<>();
            List<RawEnvelope> batch = new ArrayList<>();
            for (JsonNode rm : res.path("receivedMessages")) {
                ackIds.add(rm.path("ackId").asString());
                JsonNode m = rm.path("message");
                byte[] data = Base64.getDecoder().decode(m.path("data").asString(""));
                String topic = m.path("attributes").path("subFolder").asString(settings.subscription);
                batch.add(envelope(topic, data));
            }
            if (ackIds.isEmpty()) {
                return false;
            }
            if (isPaused()) {
                nack(ackIds);
                return false;
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                nack(ackIds);
                throw e;
            }
            ObjectNode ack = JSON.createObjectNode();
            ArrayNode ids = ack.putArray("ackIds");
            ackIds.forEach(ids::add);
            client.post("acknowledge", ack);
            return true;
        }

        private void nack(List<String> ackIds) throws Exception {
            ObjectNode body = JSON.createObjectNode().put("ackDeadlineSeconds", 0);
            ArrayNode ids = body.putArray("ackIds");
            ackIds.forEach(ids::add);
            client.post("modifyAckDeadline", body);
        }

        @Override
        protected void disconnect() {
            client = null;   // 상태 없는 HTTP. ack하지 않은 메시지는 ack 기한 뒤 다시 온다
        }
    }
}
