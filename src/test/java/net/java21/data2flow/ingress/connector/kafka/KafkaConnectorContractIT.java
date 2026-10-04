package net.java21.data2flow.ingress.connector.kafka;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DSC-09.02·09.03 TC-DSC-244 AT-DSC-20.2: Kafka 커넥터가 apache/kafka-native(KRaft) 컨테이너의 3파티션 토픽을 소비자 그룹으로 받아 계약
 * 키트 공통 시나리오를 통과한다. 확인 수 = 그룹이 커밋한 오프셋 합(AdminClient). 기록 전에는 커밋하지 않고, 기록이 실패하면 되감아 다시 받는다.
 */
class KafkaConnectorContractIT extends AbstractConnectorContractTest {

    static final String TOPIC = "kit-kafka";
    static final String GROUP = "data2flow-ingress-it-kafka";
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.9.1");
    static Admin admin;
    static KafkaProducer<byte[], byte[]> producer;

    @BeforeAll
    static void startKafka() throws Exception {
        KAFKA.start();
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
        admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"), new ByteArraySerializer(), new ByteArraySerializer());
    }

    @AfterAll
    static void stopKafka() {
        producer.close();
        admin.close();
        KAFKA.stop();
    }

    @Override
    protected SourceConnector connector() {
        return new KafkaConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("bootstrapServers", KAFKA.getBootstrapServers());
        c.putArray("topics").add(TOPIC);
        c.put("groupId", GROUP);
        c.put("batchSize", 100);
        return new SourceConfig(1, 33, SourceTypes.CONNECTOR, KafkaConnector.KEY, c, null, "data2flow-ingress-it-kafka-0");
    }

    @Override
    protected Duration timeout() {
        return Duration.ofSeconds(90);   // 그룹 가입·재조정
    }

    @Override
    protected ContractPeer peer() {
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) throws Exception {
                for (byte[] p : payloads) {
                    producer.send(new ProducerRecord<>(TOPIC, p));
                }
                producer.flush();
            }

            @Override
            public long acknowledgedCount() {
                try {
                    Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets =
                            admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                    return offsets.values().stream().filter(java.util.Objects::nonNull).mapToLong(OffsetAndMetadata::offset).sum();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }
}
