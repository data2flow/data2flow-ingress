package net.java21.data2flow.ingress.payload.service;

import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.ingress.payload.codec.PayloadCodec;
import net.java21.data2flow.ingress.payload.codec.PayloadDecodeException;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import net.java21.data2flow.ingress.payload.domain.PayloadSettings;
import net.java21.data2flow.ingress.payload.schema.SchemaInvalidException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.Optional;

/**
 * 소스 하나의 payload 변환과 토픽 메타데이터 추출(DSC-09.07·09.08). {@code data2flow.raw}에 기록하기 직전에 커넥터 스레드에서 바로
 * 실행된다 — 기록 confirm 뒤에 확인(PUBACK)하는 순서는 그대로다(DSC-09.03).
 *
 * <ol>
 *   <li>토픽 템플릿이 있으면 맞춰 본다. 맞지 않으면 {@code UNMATCHED_TOPIC} + 원본 그대로(변환도 하지 않음, BR-DSC-28).
 *       맞으면 뽑은 값을 {@code topicAttributes}로 싣는다.</li>
 *   <li>압축을 푼다. 형식이 CBOR·MessagePack·Protobuf·Avro·CSV·Sparkplug B면 구조화된 JSON으로 바꾼다. 바꾼 JSON이 {@code payload},
 *       받은 바이트는 {@code originalPayload}(무손실). JSON·TEXT·BINARY는 압축만 푼다(BINARY는 디코더 스크립트가 읽는다).</li>
 *   <li>풀 수 없으면 {@code DECODE_ERROR} + 원본 그대로. 다시 받아도 같으므로 확인은 정상대로 보낸다.</li>
 *   <li>스키마를 지금 가져올 수 없으면 {@link SchemaUnavailableException} — 호출자가 기록을 실패로 끝내 상대가 다시 보낸다.</li>
 * </ol>
 * 중복 키는 받은 바이트로 정한 것을 유지한다(이중 수신 인스턴스가 같은 키를 내야 한다). 단 푼 내용에 ChirpStack
 * {@code deduplicationId}가 있으면 그 키로 바꾼다.
 */
public final class PayloadTransformer {

    /** 결과 분류(소스 통계 EVT-DSC-03 {@code counters} 이름) */
    public static final String CONVERTED = "converted";
    public static final String DECODE_ERROR = "decodeError";
    public static final String UNMATCHED_TOPIC = "unmatchedTopic";

    /**
     * @param envelope 기록할 봉투
     * @param counter  올릴 통계 이름. 그대로 넘겼으면 null
     */
    public record Outcome(RawEnvelope envelope, String counter) {
    }

    /** 코덱을 만든다(스키마가 필요한 형식은 처음 쓸 때 core·레지스트리에서 가져온다) */
    @FunctionalInterface
    public interface CodecSource {
        PayloadCodec codec() throws PayloadDecodeException;
    }

    private final PayloadSettings settings;
    private final CodecSource codecSource;
    private final int maxDecompressedBytes;
    private final JsonMapper json;
    private volatile PayloadCodec codec;

    public PayloadTransformer(PayloadSettings settings, CodecSource codecSource, int maxDecompressedBytes, JsonMapper json) {
        this.settings = settings;
        this.codecSource = codecSource;
        this.maxDecompressedBytes = maxDecompressedBytes;
        this.json = json;
    }

    public PayloadSettings settings() {
        return settings;
    }

    public Outcome apply(RawEnvelope envelope) {
        if (settings.passthrough()) {
            return new Outcome(envelope, null);
        }
        RawEnvelope e = envelope;
        if (settings.topicTemplate() != null) {
            Optional<Map<String, String>> attributes = settings.topicTemplate().match(e.topic());
            if (attributes.isEmpty()) {
                return new Outcome(e.withIngressStatus(IngressStatus.UNMATCHED_TOPIC,
                        "토픽이 템플릿 " + settings.topicTemplate() + "에 맞지 않습니다"), UNMATCHED_TOPIC);
            }
            e = e.withTopicAttributes(attributes.get());
        }
        if (settings.compression() == net.java21.data2flow.ingress.payload.domain.Compression.NONE && !settings.converts()) {
            return new Outcome(e, null);
        }
        try {
            byte[] bytes = settings.compression().decompress(e.payload(), maxDecompressedBytes);
            if (settings.converts()) {
                JsonNode tree = codec().decode(bytes, e.topic());
                bytes = json.writeValueAsBytes(tree);
            }
            return new Outcome(e.withConvertedPayload(settings.format().name(), bytes, dedupKey(e, bytes)), CONVERTED);
        } catch (PayloadDecodeException ex) {
            return new Outcome(e.withIngressStatus(IngressStatus.DECODE_ERROR, formatName() + ": " + ex.getMessage()),
                    DECODE_ERROR);
        }
    }

    private PayloadCodec codec() throws PayloadDecodeException {
        PayloadCodec c = codec;
        if (c == null) {
            try {
                c = codecSource.codec();
            } catch (SchemaInvalidException ex) {
                throw new PayloadDecodeException("스키마를 쓸 수 없습니다: " + ex.getMessage(), ex);
            }
            codec = c;
        }
        return c;
    }

    private String formatName() {
        String f = settings.format().name();
        return settings.compression() == net.java21.data2flow.ingress.payload.domain.Compression.NONE ? f
                : f + "+" + settings.compression().name();
    }

    private static String dedupKey(RawEnvelope e, byte[] converted) {
        if (e.dedupKey().startsWith("sha256:")) {
            String detected = DedupKeys.detect(e.sourceId(), e.topic(), converted);
            if (detected.startsWith("chirpstack:")) {
                return detected;
            }
        }
        return e.dedupKey();
    }
}
