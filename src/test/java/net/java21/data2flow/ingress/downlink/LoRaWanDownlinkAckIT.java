package net.java21.data2flow.ingress.downlink;

import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ACT-03.03 TC-ACT-072(ingress 부분, ADR-054 남은 것 ①): 테스트 Mosquitto에 ChirpStack v4 모양의 소스({@code downlinkAck=true})를 두고,
 * {@code event/ack}·{@code event/txack}이 원본 스트림이 아니라 {@code data2flow.events} EVT-ACT-09로 나가는지, 업링크는 그대로
 * {@code data2flow.raw}로 가는지 본다. 발행은 이 테스트 컨테이너에만 한다(공용 브로커·ChirpStack에는 발행하지 않음, CLAUDE.md §5).
 */
class LoRaWanDownlinkAckIT extends AbstractIngressAppIT {

    static final long SOURCE = 31;
    static final String APP = UUID.randomUUID().toString();
    static final String DEV_EUI = "24e124136d151606";
    static final MessageCodec CODEC = MessageCodec.create();

    static MqttTestPublisher publisher;

    @Autowired
    RabbitTemplate rabbit;
    @Autowired
    SourceSupervisor supervisor;

    @BeforeAll
    static void setUp() {
        CORE.sources("dl-1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", brokerUrl(),
                "application/" + APP + "/device/+/event/up", "\"downlinkAck\":true") + "]");
        publisher = publisher("application/" + APP + "/device/" + DEV_EUI + "/event/up");
    }

    @AfterAll
    static void tearDown() {
        publisher.close();
    }

    @AfterEach
    void removeSource() {
        CORE.sources("dl-empty", "[]");
        configChanged();
        await().atMost(Duration.ofSeconds(30)).until(() -> supervisor.running().stream()
                .noneMatch(r -> r.definition().id() == SOURCE));
    }

    void configChanged() {
        MessageProperties props = new MessageProperties();
        props.setContentType("application/json");
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "",
                new Message(CODEC.write(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SOURCE, SOURCE, 1, 1,
                        Clock.systemUTC())), props));
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] ChirpStack event/ack·txack → EVT-ACT-09 lorawan.downlink.ack(원본 스트림에는 없음), event/up은 그대로 data2flow.raw")
    void ackAndTxAckBecomeDomainEvents() throws Exception {
        RabbitAdmin admin = new RabbitAdmin(rabbit.getConnectionFactory());
        Queue queue = new Queue("it.downlink-ack." + UUID.randomUUID(), false, false, false);
        admin.declareQueue(queue);
        admin.declareBinding(BindingBuilder.bind(queue).to(new TopicExchange(MessagingNames.EXCHANGE_EVENTS))
                .with(EventType.LORAWAN_DOWNLINK_ACK.routingKey()));
        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment())) {
            configChanged();
            await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                    .anyMatch(r -> r.definition().id() == SOURCE && r.session().status().state() == ConnectorState.CONNECTED));
            // 구독에 ack·txack이 더해졌다(업링크 토픽과 같은 애플리케이션 범위, QoS 1)
            assertThat(supervisor.running().stream().filter(r -> r.definition().id() == SOURCE).findFirst().orElseThrow()
                    .config().config().path("downlinkAck").asBoolean()).isTrue();

            String queueItemId = UUID.randomUUID().toString();
            String base = "application/" + APP + "/device/" + DEV_EUI + "/event/";
            publisher.publish(base + "txack", """
                    {"downlinkId":3712,"time":"2026-10-04T03:12:01.123Z","deviceInfo":{"tenantId":"t-1","applicationId":"%s",
                     "devEui":"%s"},"queueItemId":"%s","fCntDown":7,"gatewayId":"24e124fffef79304","txInfo":{"frequency":922100000}}"""
                    .formatted(APP, DEV_EUI, queueItemId).getBytes(StandardCharsets.UTF_8));
            publisher.publish(base + "ack", """
                    {"deduplicationId":"%s","time":"2026-10-04T03:12:04Z","deviceInfo":{"applicationId":"%s","devEui":"%s"},
                     "queueItemId":"%s","acknowledged":true,"fCntDown":7}"""
                    .formatted(UUID.randomUUID(), APP, DEV_EUI, queueItemId).getBytes(StandardCharsets.UTF_8));
            String marker = UUID.randomUUID().toString();
            publisher.publish(("{\"deviceInfo\":{\"devEui\":\"" + DEV_EUI + "\"},\"fCnt\":42,\"m\":\"" + marker + "\"}")
                    .getBytes(StandardCharsets.UTF_8));

            List<DomainEvent<LoRaWanDownlinkAck>> events = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                Message m = rabbit.receive(queue.getName(), 200);
                if (m != null) {
                    events.add(CODEC.readEvent(m.getBody(), LoRaWanDownlinkAck.class));
                }
                return events.size() >= 2;
            });
            assertThat(events).allSatisfy(e -> {
                assertThat(e.type()).isEqualTo("lorawan.downlink.ack");
                assertThat(e.organizationId()).isEqualTo(1);
                assertThat(e.payload().sourceId()).isEqualTo(SOURCE);
                assertThat(e.payload().devEui()).isEqualTo(DEV_EUI);
                assertThat(e.payload().queueItemId()).isEqualTo(queueItemId);
                assertThat(e.payload().fCntDown()).isEqualTo(7L);
            });
            LoRaWanDownlinkAck tx = events.get(0).payload();
            assertThat(tx.kind()).isEqualTo(LoRaWanDownlinkAck.Kind.TXACK);
            assertThat(tx.acknowledged()).isFalse();
            assertThat(tx.at()).isEqualTo(Instant.parse("2026-10-04T03:12:01.123Z"));
            LoRaWanDownlinkAck ack = events.get(1).payload();
            assertThat(ack.kind()).isEqualTo(LoRaWanDownlinkAck.Kind.ACK);
            assertThat(ack.acknowledged()).isTrue();

            await().atMost(Duration.ofSeconds(30)).until(() -> reader.envelopes().stream()
                    .anyMatch(e -> new String(e.payload(), StandardCharsets.UTF_8).contains(marker)));
            assertThat(reader.envelopes().stream().filter(e -> e.sourceId() == SOURCE).map(RawEnvelope::topic))
                    .isNotEmpty().allMatch(t -> t.endsWith("/event/up"));
        } finally {
            admin.deleteQueue(queue.getName());
        }
    }
}
