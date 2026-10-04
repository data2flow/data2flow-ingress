package net.java21.data2flow.ingress.connector.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
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
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * NATS JetStream 커넥터(키 {@code nats-jetstream}, connectors.md §2 jnats). 지속(durable) pull 소비자로 최대 {@code batchSize}건을
 * 가져와 모두 기록(confirm)한 뒤에 하나씩 ack한다(AFTER_WRITE). 기록이 실패하면 nak으로 바로 다시 받는다. 같은 durable을 쓰는 인스턴스가
 * 나눠 받는다(SCALABLE). 소비자 정의(명시적 ack)는 없으면 만들지만 스트림·메시지는 만들지 않는다(구독 전용).
 */
public class NatsJetStreamConnector implements SourceConnector {

    public static final String KEY = "nats-jetstream";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final PollingOptions options;
    private final Clock clock;

    public NatsJetStreamConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "NATS JetStream", "1.0.0", ConnectorCategory.QUEUE,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.TOKEN, AuthMethod.MTLS),
                Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.BINARY, PayloadFormat.CBOR,
                        PayloadFormat.MSGPACK, PayloadFormat.PROTOBUF),
                AckMode.AFTER_WRITE, ScalingMode.SCALABLE, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        return new StagedConnectionTest<Handle>(s.uri.getHost(), s.uri.getPort() > 0 ? s.uri.getPort() : 4222, false,
                null, true, options.testTimeout(), ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> new Handle(Nats.connect(natsOptions(s, config, "data2flow-ingress-test", remaining))),
                        (h, remaining) -> {
                            // 스트림이 주제를 받는지만 확인한다. 미리보기는 소비 위치를 바꾸지 않도록 하지 않는다
                            String stream = h.connection.jetStreamManagement().getStreamNames(s.subject).stream()
                                    .findFirst().orElseThrow(() -> new IllegalStateException("주제를 받는 스트림이 없습니다"));
                            if (s.stream != null && !s.stream.equals(stream)) {
                                throw new IllegalStateException("주제를 받는 스트림이 설정과 다릅니다: " + stream);
                            }
                            return List.<ConnectionTestResult.Preview>of();
                        }, false);
    }

    private record Handle(Connection connection) implements AutoCloseable {
        @Override
        public void close() throws InterruptedException {
            connection.close();
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    static Options natsOptions(Settings s, SourceConfig config, String name, Duration timeout) throws Exception {
        Cfg cfg = Cfg.of(config);
        Options.Builder b = new Options.Builder().server(s.uri.toString()).connectionName(name)
                .connectionTimeout(timeout).maxReconnects(0).noReconnect();
        if (s.username != null) {
            b.userInfo(s.username, cfg.requiredSecret("PASSWORD").reveal());
        } else if (cfg.secret("TOKEN") != null) {
            b.token(cfg.reveal("TOKEN").toCharArray());
        }
        if ("tls".equalsIgnoreCase(s.uri.getScheme())) {
            b.sslContext(ConnectorTls.from(cfg).context());
        }
        return b.build();
    }

    record Settings(URI uri, String stream, String subject, String durable, String username, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            URI uri = c.uri("url");
            if (!Set.of("nats", "tls").contains(uri.getScheme().toLowerCase(java.util.Locale.ROOT))) {
                throw InvalidSettingsException.config("url");
            }
            String durable = c.text("durable", "data2flow-ingress-" + config.sourceId());
            if (!durable.matches("[A-Za-z0-9_-]{1,64}")) {
                throw InvalidSettingsException.config("durable");
            }
            return new Settings(uri, c.text("stream", null), c.required("subject"), durable, c.text("username", null),
                    c.integer("batchSize", 100, 1, 1000));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Connection connection;
        private JetStreamSubscription subscription;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            connection = Nats.connect(natsOptions(settings, config,
                    config.clientId() == null ? "data2flow-ingress" : config.clientId(), options.connectTimeout()));
            JetStream js = connection.jetStream();
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(settings.durable)
                    .ackPolicy(AckPolicy.Explicit).deliverPolicy(DeliverPolicy.All).ackWait(Duration.ofSeconds(60))
                    .maxAckPending(Math.max(1000, settings.batchSize * 4L)).build();
            PullSubscribeOptions.Builder pso = PullSubscribeOptions.builder().configuration(cc);
            if (settings.stream != null) {
                pso.stream(settings.stream);
            }
            subscription = js.subscribe(settings.subject, pso.build());
        }

        @Override
        protected boolean pollOnce() throws Exception {
            List<Message> messages = subscription.fetch(settings.batchSize,
                    Duration.ofMillis(Math.max(100, options.idleInterval().toMillis())));
            if (messages.isEmpty()) {
                return false;
            }
            if (isPaused()) {   // 일시정지 중에 받은 것은 넘기지 않고 돌려놓는다
                messages.forEach(Message::nak);
                return false;
            }
            List<RawEnvelope> batch = new ArrayList<>(messages.size());
            for (Message m : messages) {
                batch.add(envelope(m.getSubject(), m.getData() == null ? new byte[0] : m.getData()));
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                messages.forEach(Message::nak);
                throw e;
            }
            for (Message m : messages) {
                m.ackSync(Duration.ofSeconds(5));
            }
            return true;
        }

        @Override
        protected void disconnect() {
            try {
                if (connection != null) {
                    connection.close();   // ack하지 않은 메시지는 ackWait 뒤 다시 온다
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                connection = null;
                subscription = null;
            }
        }
    }
}
