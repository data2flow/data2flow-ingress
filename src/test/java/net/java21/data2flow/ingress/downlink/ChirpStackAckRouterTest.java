package net.java21.data2flow.ingress.downlink;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.ingress.downlink.service.ChirpStackAckRouter;
import net.java21.data2flow.ingress.source.event.SourceEventPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** ACT-03.03 TC-ACT-072(ingress 부분): ChirpStack 다운링크 결과 해석과 "발행 확인 뒤에만 끝남"(MQTT 확인 순서) */
class ChirpStackAckRouterTest {

    static final Instant T = Instant.parse("2026-10-04T03:12:00Z");
    static final MessageCodec CODEC = MessageCodec.create();

    /** 보낸 메시지와 확인 대기(CorrelationData)를 잡아 두는 RabbitTemplate */
    static final class CapturingTemplate extends RabbitTemplate {
        final List<Message> sent = new CopyOnWriteArrayList<>();
        final List<CorrelationData> pending = new CopyOnWriteArrayList<>();
        final List<String> keys = new CopyOnWriteArrayList<>();

        @Override
        public void send(String exchange, String routingKey, Message message, CorrelationData correlationData) {
            keys.add(routingKey);
            sent.add(message);
            pending.add(correlationData);
        }
    }

    final CapturingTemplate rabbit = new CapturingTemplate();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final ChirpStackAckRouter router = new ChirpStackAckRouter(new SourceEventPublisher(rabbit, CODEC, meters),
            MessageCodec.newMapper(), meters, Clock.fixed(T, ZoneOffset.UTC));

    static RawEnvelope envelope(String topic, String json) {
        return new RawEnvelope(1, UUID.randomUUID(), 1, 9, SourceTypes.MQTT_SUBSCRIBE, topic, json.getBytes(StandardCharsets.UTF_8), T,
                "ingress-0", "k-" + UUID.randomUUID(), false, null, null);
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] ack·txack 토픽만 맡는다(업링크·다른 토픽은 원본 스트림)")
    void matchesOnlyDownlinkResultTopics() {
        assertThat(router.matches(envelope("application/1/device/24e124136d151606/event/ack", "{}"))).isTrue();
        assertThat(router.matches(envelope("application/1/device/24e124136d151606/event/txack", "{}"))).isTrue();
        assertThat(router.matches(envelope("application/1/device/24e124136d151606/event/up", "{}"))).isFalse();
        assertThat(router.matches(envelope("application/1/device/24e124136d151606/event/ack/x", "{}"))).isFalse();
        assertThat(router.matches(envelope("devices/esp32/command/ack", "{}"))).isFalse();
        assertThat(router.matches(envelope(null, "{}"))).isFalse();
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] 발행 확인(confirm)이 오기 전에는 끝나지 않고(PUBACK 보류), ack면 끝나고 nack면 실패한다")
    void completesOnlyAfterPublisherConfirm() {
        CompletableFuture<Void> first = router.route(envelope("application/1/device/24E124136D151606/event/ack", """
                {"deduplicationId":"d-1","time":"2026-10-04T03:12:04Z","deviceInfo":{"devEui":"24E124136D151606"},
                 "queueItemId":"q-1","acknowledged":true,"fCntDown":"7"}""")).toCompletableFuture();
        assertThat(first).isNotDone();
        assertThat(rabbit.keys).containsExactly("lorawan.downlink.ack");
        LoRaWanDownlinkAck ack = CODEC.readEvent(rabbit.sent.getFirst().getBody(), LoRaWanDownlinkAck.class).payload();
        assertThat(ack).isEqualTo(LoRaWanDownlinkAck.ack(9, "24e124136d151606", "q-1", true, 7L, Instant.parse("2026-10-04T03:12:04Z")));
        rabbit.pending.getFirst().getFuture().complete(new CorrelationData.Confirm(true, null));
        assertThat(first).isCompleted();

        CompletableFuture<Void> second = router.route(envelope("application/1/device/24e124136d151606/event/ack",
                "{\"queueItemId\":\"q-2\"}")).toCompletableFuture();
        rabbit.pending.get(1).getFuture().complete(new CorrelationData.Confirm(false, "queue full"));
        assertThat(second).isCompletedExceptionally();
        LoRaWanDownlinkAck notAcked = CODEC.readEvent(rabbit.sent.get(1).getBody(), LoRaWanDownlinkAck.class).payload();
        assertThat(notAcked.acknowledged()).isFalse();   // protobuf JSON은 false를 생략한다
        assertThat(notAcked.fCntDown()).isNull();
        assertThat(notAcked.at()).isEqualTo(T);           // time이 없으면 수신 시각
        assertThat(notAcked.devEui()).isEqualTo("24e124136d151606");   // deviceInfo가 없으면 토픽에서
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] txack는 TXACK(acknowledged=false), 큐 항목 ID 없음·JSON 아님은 버리고 바로 끝난다(무한 재전송 방지)")
    void txAckAndInvalid() {
        router.route(envelope("application/1/device/24e124136d151606/event/txack", """
                {"downlinkId":1,"time":"2026-10-04T03:12:01Z","queueItemId":"q-3","fCntDown":8,"gatewayId":"gw"}"""));
        LoRaWanDownlinkAck tx = CODEC.readEvent(rabbit.sent.getFirst().getBody(), LoRaWanDownlinkAck.class).payload();
        assertThat(tx.kind()).isEqualTo(LoRaWanDownlinkAck.Kind.TXACK);
        assertThat(tx.acknowledged()).isFalse();
        assertThat(tx.fCntDown()).isEqualTo(8L);

        assertThat(router.route(envelope("application/1/device/24e124136d151606/event/ack", "{\"acknowledged\":true}"))
                .toCompletableFuture()).isCompleted();
        assertThat(router.route(envelope("application/1/device/24e124136d151606/event/ack", "not json"))
                .toCompletableFuture()).isCompleted();
        assertThat(router.route(envelope("application/1/device/not-an-eui/event/ack", "{\"queueItemId\":\"q-4\"}"))
                .toCompletableFuture()).isCompleted();
        assertThat(rabbit.sent).hasSize(1);
        assertThat(meters.counter("data2flow.ingress.downlink.acks", "result", "invalid").count()).isEqualTo(3);
    }
}
