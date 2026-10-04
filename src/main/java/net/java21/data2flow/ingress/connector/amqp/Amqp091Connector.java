package net.java21.data2flow.ingress.connector.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
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
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * AMQP 0-9-1 소비 커넥터(키 {@code amqp091}, DSC-09 카탈로그 "메시지 큐", connectors.md §2 RabbitMQ Java Client).
 *
 * <p>큐에서 {@code basic.get}(수동 확인)으로 최대 {@code batchSize}건을 가져와 모두 기록(confirm)한 뒤에 {@code basic.ack(multiple)}를
 * 보낸다(AFTER_WRITE, AT-DSC-20.1). 기록이 실패하면 {@code basic.nack(requeue)}로 돌려놓는다. 여러 ingress가 같은 큐를 나눠 받는 경쟁
 * 소비(SCALABLE)다. 큐·교환기를 만들거나 메시지를 발행하지 않는다(구독 전용, CLAUDE.md §5).
 */
public class Amqp091Connector implements SourceConnector {

    public static final String KEY = "amqp091";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final PollingOptions options;
    private final Clock clock;

    public Amqp091Connector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "AMQP 0-9-1 (RabbitMQ)", "1.0.0", ConnectorCategory.QUEUE,
                Set.of(AuthMethod.USER_PASSWORD, AuthMethod.MTLS),
                Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.BINARY, PayloadFormat.CBOR,
                        PayloadFormat.MSGPACK, PayloadFormat.PROTOBUF, PayloadFormat.AVRO),
                AckMode.AFTER_WRITE, ScalingMode.SCALABLE, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        ConnectorTls tls = ConnectorTls.from(Cfg.of(config));
        javax.net.ssl.SSLContext ssl = null;
        try {
            ssl = s.tls ? tls.context() : null;
        } catch (Exception e) {
            ssl = null;
        }
        return new StagedConnectionTest<Connection>(s.host, s.port, false, ssl, !tls.insecure(), options.testTimeout(),
                ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> factory(s, config, remaining).newConnection("data2flow-ingress-test"),
                        (connection, remaining) -> {
                            try (Channel ch = connection.createChannel()) {
                                ch.queueDeclarePassive(s.queue);
                                List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                                long last = -1;
                                for (int i = 0; i < ConnectionTestResult.MAX_PREVIEW; i++) {
                                    GetResponse r = ch.basicGet(s.queue, false);
                                    if (r == null) {
                                        break;
                                    }
                                    last = r.getEnvelope().getDeliveryTag();
                                    preview.add(StagedConnectionTest.preview(clock, topic(s, r), r.getBody()));
                                }
                                if (last >= 0) {
                                    ch.basicNack(last, true, true);   // 미리보기는 확인하지 않고 돌려놓는다
                                }
                                return preview;
                            }
                        }, false);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    static ConnectionFactory factory(Settings s, SourceConfig config, Duration timeout) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(s.host);
        f.setPort(s.port);
        f.setVirtualHost(s.vhost);
        f.setConnectionTimeout((int) Math.min(Integer.MAX_VALUE, Math.max(1000, timeout.toMillis())));
        f.setAutomaticRecoveryEnabled(false);
        f.setRequestedHeartbeat(30);
        Cfg cfg = Cfg.of(config);
        if (s.username != null) {
            f.setUsername(s.username);
            f.setPassword(cfg.requiredSecret("PASSWORD").reveal());
        }
        if (s.tls) {
            f.useSslProtocol(ConnectorTls.from(cfg).context());
            if (!ConnectorTls.from(cfg).insecure()) {
                f.enableHostnameVerification();
            }
            if (ConnectorTls.from(cfg).mutual() && s.username == null) {
                f.setSaslConfig(com.rabbitmq.client.DefaultSaslConfig.EXTERNAL);
            }
        }
        return f;
    }

    static String topic(Settings s, GetResponse r) {
        String rk = r.getEnvelope().getRoutingKey();
        return rk == null || rk.isBlank() ? s.queue : rk;
    }

    /** 설정({@code amqp091.schema.json}) */
    record Settings(String host, int port, boolean tls, String vhost, String queue, String username, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            URI uri = c.uri("url");
            boolean tls = "amqps".equalsIgnoreCase(uri.getScheme());
            if (!tls && !"amqp".equalsIgnoreCase(uri.getScheme())) {
                throw net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException.config("url");
            }
            int port = uri.getPort() > 0 ? uri.getPort() : tls ? 5671 : 5672;
            return new Settings(uri.getHost(), port, tls, c.text("vhost", "/"), c.required("queue"),
                    c.text("username", null), c.integer("batchSize", 100, 1, 1000));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Connection connection;
        private Channel channel;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), options.idleInterval(), options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            connection = factory(settings, config, options.connectTimeout())
                    .newConnection(config.clientId() == null ? "data2flow-ingress" : config.clientId());
            channel = connection.createChannel();
            channel.queueDeclarePassive(settings.queue);
        }

        @Override
        protected boolean pollOnce() throws Exception {
            List<RawEnvelope> batch = new ArrayList<>();
            long lastTag = -1;
            for (int i = 0; i < settings.batchSize && !isPaused(); i++) {
                GetResponse r = channel.basicGet(settings.queue, false);
                if (r == null) {
                    break;
                }
                lastTag = r.getEnvelope().getDeliveryTag();
                batch.add(envelope(topic(settings, r), r.getBody()));
            }
            if (lastTag < 0) {
                return false;
            }
            if (isPaused()) {   // 일시정지 중에 받은 것은 넘기지 않고 돌려놓는다
                channel.basicNack(lastTag, true, true);
                return false;
            }
            try {
                writeAll(batch);
            } catch (net.java21.data2flow.ingress.connector.common.WriteFailedException e) {
                channel.basicNack(lastTag, true, true);
                throw e;
            }
            channel.basicAck(lastTag, true);
            return true;
        }

        @Override
        protected void disconnect() {
            try {
                if (connection != null && connection.isOpen()) {
                    connection.abort(2000);   // 확인하지 않은 메시지는 큐로 돌아간다
                }
            } finally {
                connection = null;
                channel = null;
            }
        }
    }
}
