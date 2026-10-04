package net.java21.data2flow.ingress.connector.kafka;

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
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.common.WriteFailedException;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka 소비자 그룹 커넥터(키 {@code kafka}, connectors.md §2 kafka-clients). 자동 커밋을 끄고, 가져온 레코드를 모두 기록(confirm)한 뒤에
 * 오프셋을 동기 커밋한다(AFTER_WRITE, AT-DSC-20.2: 기록 전에 죽으면 커밋하지 않은 위치부터 다시 받는다). 기록이 실패하면 파티션을 커밋한
 * 위치로 되감는다. 같은 그룹의 ingress 인스턴스가 파티션을 나눠 받는다(SCALABLE). 인증: 없음·SASL PLAIN·SCRAM-SHA-256/512, TLS.
 * 생산자(발행) 코드는 없다.
 */
public class KafkaConnector implements SourceConnector {

    public static final String KEY = "kafka";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final PollingOptions options;
    private final Clock clock;

    public KafkaConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "Apache Kafka", "1.0.0", ConnectorCategory.QUEUE,
                Set.of(AuthMethod.NONE, AuthMethod.SASL_PLAIN, AuthMethod.SASL_SCRAM_256, AuthMethod.SASL_SCRAM_512,
                        AuthMethod.MTLS),
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
        String first = s.bootstrap.split(",")[0].trim();
        String host = first.substring(0, first.lastIndexOf(':'));
        int port = Integer.parseInt(first.substring(first.lastIndexOf(':') + 1));
        return new StagedConnectionTest<Consumer>(host, port, false, null, true, options.testTimeout(),
                ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> {
                    Properties p = properties(s, config, "data2flow-ingress-test-" + Long.toHexString(System.nanoTime()));
                    KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
                    c.listTopics(remaining);   // 인증·메타데이터
                    return new Consumer(c);
                }, (h, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    List<TopicPartition> parts = new ArrayList<>();
                    for (String t : s.topics) {
                        var infos = h.consumer.partitionsFor(t, remaining);
                        if (infos == null || infos.isEmpty()) {
                            throw new IllegalStateException("토픽이 없습니다: " + t);
                        }
                        infos.forEach(i -> parts.add(new TopicPartition(i.topic(), i.partition())));
                    }
                    h.consumer.assign(parts);   // 그룹에 들어가지 않고 끝에서 10건만 본다(커밋 없음)
                    Map<TopicPartition, Long> end = h.consumer.endOffsets(parts);
                    end.forEach((tp, off) -> h.consumer.seek(tp, Math.max(0, off - ConnectionTestResult.MAX_PREVIEW)));
                    for (ConsumerRecord<byte[], byte[]> r : h.consumer.poll(Duration.ofSeconds(1))) {
                        if (preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                            preview.add(StagedConnectionTest.preview(clock, r.topic(), r.value() == null ? new byte[0] : r.value()));
                        }
                    }
                    return preview;
                }, false);
    }

    private record Consumer(KafkaConsumer<byte[], byte[]> consumer) implements AutoCloseable {
        @Override
        public void close() {
            consumer.close(Duration.ofSeconds(2));
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    static Properties properties(Settings s, SourceConfig config, String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, s.groupId);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, s.startFrom);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(s.batchSize));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put("security.protocol", s.securityProtocol);
        if (s.securityProtocol.startsWith("SASL")) {
            Cfg cfg = Cfg.of(config);
            String password = cfg.requiredSecret("PASSWORD").reveal();
            String module = s.saslMechanism.equals("PLAIN")
                    ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                    : "org.apache.kafka.common.security.scram.ScramLoginModule";
            p.put("sasl.mechanism", s.saslMechanism);
            p.put("sasl.jaas.config", module + " required username=\"" + escape(s.username) + "\" password=\""
                    + escape(password) + "\";");
        }
        return p;
    }

    private static String escape(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** 설정({@code kafka.schema.json}) */
    record Settings(String bootstrap, List<String> topics, String groupId, String securityProtocol, String saslMechanism,
                    String username, String startFrom, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            String bootstrap = c.required("bootstrapServers");
            if (!bootstrap.matches("[^\\s,]+:\\d{1,5}(,[^\\s,]+:\\d{1,5})*")) {
                throw InvalidSettingsException.config("bootstrapServers");
            }
            List<String> topics = c.strings("topics");
            if (topics.isEmpty()) {
                throw InvalidSettingsException.config("topics");
            }
            String protocol = c.text("securityProtocol", "PLAINTEXT").toUpperCase(Locale.ROOT);
            if (!Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol)) {
                throw InvalidSettingsException.config("securityProtocol");
            }
            String mechanism = c.text("saslMechanism", "SCRAM-SHA-512").toUpperCase(Locale.ROOT);
            if (!Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").contains(mechanism)) {
                throw InvalidSettingsException.config("saslMechanism");
            }
            String username = c.text("username", null);
            if (protocol.startsWith("SASL") && username == null) {
                throw InvalidSettingsException.config("username");
            }
            String start = c.text("startFrom", "earliest").toLowerCase(Locale.ROOT);
            if (!Set.of("earliest", "latest").contains(start)) {
                throw InvalidSettingsException.config("startFrom");
            }
            String group = c.text("groupId", "data2flow-ingress-" + config.sourceId());
            return new Settings(bootstrap, topics, group, protocol, mechanism, username, start,
                    c.integer("batchSize", 500, 1, 10_000));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private KafkaConsumer<byte[], byte[]> consumer;
        private boolean consumerPaused;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() {
            consumer = new KafkaConsumer<>(properties(settings, config,
                    config.clientId() == null ? "data2flow-ingress" : config.clientId()));
            consumer.subscribe(settings.topics);
            consumerPaused = false;
        }

        @Override
        protected boolean pollOnce() throws Exception {
            if (consumerPaused) {
                consumer.resume(consumer.paused());
                consumerPaused = false;
            }
            ConsumerRecords<byte[], byte[]> records;
            try {
                records = consumer.poll(Duration.ofMillis(Math.max(50, options.idleInterval().toMillis())));
            } catch (WakeupException e) {
                return false;
            }
            if (records.isEmpty()) {
                return false;
            }
            Map<TopicPartition, Long> first = new HashMap<>();
            Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
            List<RawEnvelope> batch = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                first.putIfAbsent(tp, r.offset());
                next.put(tp, new OffsetAndMetadata(r.offset() + 1));
                batch.add(envelope(r.topic(), r.value() == null ? new byte[0] : r.value()));
            }
            if (isPaused()) {
                first.forEach(consumer::seek);   // 일시정지 중에 받은 것은 넘기지 않고 되감는다
                return false;
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                first.forEach(consumer::seek);   // 커밋하지 않고 같은 위치부터 다시 받는다
                throw e;
            }
            consumer.commitSync(next);
            return true;
        }

        @Override
        protected void pausedTick() {
            if (consumer == null) {
                return;
            }
            consumer.pause(consumer.assignment());   // 재조정으로 새로 받은 파티션도 멈춘다
            consumerPaused = true;
            try {
                // 그룹 멤버십 유지. 멈추기 전에 배정된 파티션의 레코드가 오면 넘기지 않고 되감는다
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                Map<TopicPartition, Long> first = new HashMap<>();
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    first.putIfAbsent(new TopicPartition(r.topic(), r.partition()), r.offset());
                }
                first.forEach(consumer::seek);
            } catch (WakeupException ignored) {
                // close 중
            }
        }

        @Override
        protected void interruptClient(Thread worker) {
            KafkaConsumer<byte[], byte[]> c = consumer;
            if (c != null) {
                c.wakeup();
            } else {
                worker.interrupt();
            }
        }

        @Override
        protected void disconnect() {
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (RuntimeException ignored) {
                    // 닫는 중 오류는 무시
                }
                consumer = null;
            }
        }
    }
}
