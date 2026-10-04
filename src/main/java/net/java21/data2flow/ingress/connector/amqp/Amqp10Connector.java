package net.java21.data2flow.ingress.connector.amqp;

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
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * AMQP 1.0 소비 커넥터(키 {@code amqp10}, connectors.md §2 Apache Qpid ProtonJ2). ActiveMQ Artemis, Azure Service Bus, RabbitMQ 4
 * ({@code /queues/{이름}}), Azure IoT Hub의 Event Hubs 호환 엔드포인트처럼 AMQP 1.0을 말하는 상대에서 받는다.
 *
 * <p>자동 수락을 끄고 credit 창({@code batchSize})만큼 받아 모두 기록(confirm)한 뒤에 {@code accepted}로 정산한다(AFTER_WRITE). 기록이
 * 실패하면 {@code released}로 돌려놓아 다시 받는다. 경쟁 소비라 SCALABLE. 송신 링크를 열지 않는다(구독 전용).
 */
public class Amqp10Connector implements SourceConnector {

    public static final String KEY = "amqp10";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final PollingOptions options;
    private final Clock clock;

    public Amqp10Connector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "AMQP 1.0", "1.0.0", ConnectorCategory.QUEUE,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.TOKEN, AuthMethod.MTLS),
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
        javax.net.ssl.SSLContext ssl;
        try {
            ssl = s.tls ? tls.context() : null;
        } catch (Exception e) {
            ssl = null;
        }
        return new StagedConnectionTest<Handle>(s.host, s.port, false, ssl, !tls.insecure(), options.testTimeout(),
                ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> {
                    Client client = Client.create(new ClientOptions().id("data2flow-ingress-test"));
                    Connection c = client.connect(s.host, s.port, connectionOptions(s, config));
                    c.openFuture().get(remaining.toMillis(), TimeUnit.MILLISECONDS);
                    return new Handle(client, c);
                }, (h, remaining) -> {
                    Receiver r = h.connection.openReceiver(s.address, new ReceiverOptions().autoAccept(false)
                            .creditWindow(ConnectionTestResult.MAX_PREVIEW));
                    r.openFuture().get(remaining.toMillis(), TimeUnit.MILLISECONDS);
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    Delivery d;
                    while (preview.size() < ConnectionTestResult.MAX_PREVIEW && (d = r.receive(500, TimeUnit.MILLISECONDS)) != null) {
                        preview.add(StagedConnectionTest.preview(clock, s.address, body(d)));
                        d.release();   // 미리보기는 확인하지 않는다
                    }
                    r.close();
                    return preview;
                }, false);
    }

    /** 연결 테스트 핸들 */
    private record Handle(Client client, Connection connection) implements AutoCloseable {
        @Override
        public void close() {
            connection.close();
            client.close();
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    static ConnectionOptions connectionOptions(Settings s, SourceConfig config) throws Exception {
        Cfg cfg = Cfg.of(config);
        ConnectionOptions o = new ConnectionOptions();
        o.openTimeout(10_000);
        o.idleTimeout(60_000);
        if (s.username != null) {
            o.user(s.username);
            String password = cfg.reveal("PASSWORD");
            o.password(password != null ? password : cfg.requiredSecret("TOKEN").reveal());
        }
        if (s.tls) {
            ConnectorTls tls = ConnectorTls.from(cfg);
            o.sslEnabled(true);
            o.sslOptions().sslContextOverride(tls.context());
            o.sslOptions().verifyHost(!tls.insecure());
        }
        return o;
    }

    /** 본문: Data 섹션(byte[])은 그대로, AmqpValue 문자열은 UTF-8 */
    static byte[] body(Delivery d) throws Exception {
        Message<Object> m = d.message();
        Object body = m.body();
        if (body instanceof byte[] bytes) {
            return bytes;
        }
        return body == null ? new byte[0] : String.valueOf(body).getBytes(StandardCharsets.UTF_8);
    }

    record Settings(String host, int port, boolean tls, String address, String username, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            URI uri = c.uri("url");
            boolean tls = "amqps".equalsIgnoreCase(uri.getScheme());
            if (!tls && !"amqp".equalsIgnoreCase(uri.getScheme())) {
                throw InvalidSettingsException.config("url");
            }
            return new Settings(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : tls ? 5671 : 5672, tls,
                    c.required("address"), c.text("username", null), c.integer("batchSize", 100, 1, 1000));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Client client;
        private Connection connection;
        private Receiver receiver;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            client = Client.create(new ClientOptions().id(config.clientId() == null ? "data2flow-ingress" : config.clientId()));
            connection = client.connect(settings.host, settings.port, connectionOptions(settings, config));
            connection.openFuture().get(options.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
            receiver = connection.openReceiver(settings.address,
                    new ReceiverOptions().autoAccept(false).creditWindow(settings.batchSize));
            receiver.openFuture().get(options.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        protected boolean pollOnce() throws Exception {
            List<Delivery> deliveries = new ArrayList<>();
            List<RawEnvelope> batch = new ArrayList<>();
            Delivery d = receiver.receive(Math.max(50, options.idleInterval().toMillis()), TimeUnit.MILLISECONDS);
            while (d != null) {
                deliveries.add(d);
                Message<Object> m = d.message();
                String subject = m.subject();
                batch.add(envelope(subject == null || subject.isBlank() ? settings.address : subject, body(d)));
                if (deliveries.size() >= settings.batchSize || isPaused()) {
                    break;
                }
                d = receiver.tryReceive();
            }
            if (deliveries.isEmpty()) {
                return false;
            }
            if (isPaused()) {   // 일시정지 중에 받은 것은 넘기지 않고 돌려놓는다
                for (Delivery x : deliveries) {
                    x.release();
                }
                return false;
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                for (Delivery x : deliveries) {
                    x.release();
                }
                throw e;
            }
            for (Delivery x : deliveries) {
                x.accept();
            }
            return true;
        }

        @Override
        protected void disconnect() {
            try {
                if (connection != null) {
                    connection.close();   // 정산하지 않은 전달은 상대가 다시 보낸다
                }
                if (client != null) {
                    client.close();
                }
            } catch (RuntimeException ignored) {
                // 닫는 중 오류는 무시
            } finally {
                connection = null;
                client = null;
                receiver = null;
            }
        }
    }
}
