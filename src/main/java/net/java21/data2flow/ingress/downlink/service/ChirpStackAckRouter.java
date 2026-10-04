package net.java21.data2flow.ingress.downlink.service;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.ingress.source.event.SourceEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ChirpStack v4 다운링크 결과를 EVT-ACT-09 {@code lorawan.downlink.ack}로 바꾼다(ACT-03.03, ADR-054 남은 것 ①).
 *
 * <p>MQTT 소스가 받은 {@code application/{appId}/device/{devEui}/event/ack}(확인형 다운링크의 기기 확인)·{@code …/event/txack}(게이트웨이
 * 송신)는 텔레메트리가 아니므로 원본 스트림({@code data2flow.raw})에 쓰지 않고 이 라우터가 {@code data2flow.events}에 낸다. 같은 수신
 * 경로를 타므로 MQTT 확인(PUBACK)은 브로커가 이벤트 발행을 확인(publisher confirm)한 뒤에만 나간다. 발행이 실패하면 세션이 끊고 다시
 * 접속해 브로커 재전송을 받는다(reliability-and-ha.md ②).
 *
 * <p>ChirpStack JSON(protobuf JSON 매핑)은 기본값을 생략할 수 있어 {@code acknowledged}가 없으면 false, {@code fCntDown}이 없으면 null로
 * 읽는다. 큐 항목 ID가 없거나 JSON이 아니면 다시 받아도 같으므로 기록만 하고 확인한다(무한 재전송 방지). 구독만 하고 발행하지 않는다
 * (CLAUDE.md §5).
 */
public class ChirpStackAckRouter {

    /** {@code application/{appId}/device/{devEui}/event/{ack|txack}} */
    static final Pattern TOPIC = Pattern.compile("^application/[^/]+/device/([^/]+)/event/(ack|txack)$");
    private static final Pattern DEV_EUI = Pattern.compile("^[0-9a-f]{16}$");
    private static final Logger log = LoggerFactory.getLogger(ChirpStackAckRouter.class);

    private final SourceEventPublisher events;
    private final JsonMapper json;
    private final MeterRegistry meters;
    private final Clock clock;

    public ChirpStackAckRouter(SourceEventPublisher events, JsonMapper json, MeterRegistry meters, Clock clock) {
        this.events = events;
        this.json = json;
        this.meters = meters;
        this.clock = clock;
    }

    /** 이 라우터가 맡는 메시지인가(ChirpStack 다운링크 결과 토픽) */
    public boolean matches(RawEnvelope envelope) {
        return envelope.topic() != null && TOPIC.matcher(envelope.topic()).matches();
    }

    /** 이벤트로 낸다. 결과가 정상으로 끝나야 원본을 확인한다 */
    public CompletionStage<Void> route(RawEnvelope envelope) {
        Optional<LoRaWanDownlinkAck> ack = parse(envelope);
        if (ack.isEmpty()) {
            meters.counter("data2flow.ingress.downlink.acks", "result", "invalid").increment();
            return CompletableFuture.completedFuture(null);
        }
        DomainEvent<LoRaWanDownlinkAck> event = DomainEvent.of(EventType.LORAWAN_DOWNLINK_ACK, envelope.organizationId(), ack.get(),
                null, clock);
        String kind = ack.get().effectiveKind().name();
        return events.publishConfirmed(event).thenRun(() -> meters.counter("data2flow.ingress.downlink.acks", "result", kind)
                .increment());
    }

    Optional<LoRaWanDownlinkAck> parse(RawEnvelope envelope) {
        Matcher m = TOPIC.matcher(envelope.topic());
        if (!m.matches()) {
            return Optional.empty();
        }
        JsonNode body;
        try {
            body = json.readTree(envelope.payload());
        } catch (JacksonException e) {
            log.warn("소스 {} 다운링크 결과가 JSON이 아니라 버립니다(topic {}): {}", envelope.sourceId(), envelope.topic(), e.getOriginalMessage());
            return Optional.empty();
        }
        String queueItemId = text(body, "queueItemId");
        String devEui = Optional.ofNullable(text(body.path("deviceInfo"), "devEui")).orElse(m.group(1)).toLowerCase(Locale.ROOT);
        if (queueItemId == null || !DEV_EUI.matcher(devEui).matches()) {
            log.warn("소스 {} 다운링크 결과에 큐 항목 ID 또는 DevEUI가 없어 버립니다(topic {})", envelope.sourceId(), envelope.topic());
            return Optional.empty();
        }
        Long fCntDown = body.path("fCntDown").isNumber() || body.path("fCntDown").isString()
                ? parseLong(body.path("fCntDown").asString()) : null;
        Instant at = time(body, envelope.receivedAt());
        return Optional.of("txack".equals(m.group(2))
                ? LoRaWanDownlinkAck.txAck(envelope.sourceId(), devEui, queueItemId, fCntDown, at)
                : LoRaWanDownlinkAck.ack(envelope.sourceId(), devEui, queueItemId, body.path("acknowledged").asBoolean(false),
                fCntDown, at));
    }

    private static Instant time(JsonNode body, Instant fallback) {
        String t = text(body, "time");
        if (t == null) {
            return fallback;
        }
        try {
            return Instant.parse(t);
        } catch (DateTimeParseException e) {
            return fallback;
        }
    }

    private static Long parseLong(String v) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || !v.isValueNode() || v.isNull() || v.asString("").isBlank() ? null : v.asString();
    }
}
