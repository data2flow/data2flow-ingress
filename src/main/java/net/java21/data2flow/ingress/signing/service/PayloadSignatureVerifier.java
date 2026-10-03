package net.java21.data2flow.ingress.signing.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.message.PlatformBrokerSignature;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SignatureStatus;
import net.java21.data2flow.contracts.message.SourceTypes;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * 플랫폼 브로커 기기 payload 서명 검증(DSC-03.03·03.05, BR-DSC-12, ADR-042). 공용 브로커는 토픽 ACL을 걸 수 없어서(ADR-029)
 * 토픽 {@code devices/{deviceKey}/…}의 기기 키로 서명을 확인한다. 결과는 {@link RawEnvelope#signatureStatus()}로 pipeline에 넘긴다.
 *
 * <ul>
 *   <li>키가 있고 서명이 맞음 → VERIFIED, payload는 서명 접두사를 뗀 본문</li>
 *   <li>키가 있는데 서명이 없거나 틀림(다른 기기의 키 포함) → INVALID, payload는 받은 그대로. 지표
 *       {@code data2flow_ingest_signature_rejected_total}(reason=missing|mismatch) 증가. 원본은 스트림에 기록하고 pipeline이
 *       {@code DEVICE_SIGNATURE_INVALID}로 거부한다(원본 보관, 저장 0건)</li>
 *   <li>키가 없음(승인 전·폐기됨) → UNSIGNED, 서명 접두사가 있으면 뗀다. pipeline이 승인 대기면 quality 2로 격리, 승인된 기기면 거부</li>
 * </ul>
 * PLATFORM_BROKER가 아닌 소스는 건드리지 않는다.
 */
public class PayloadSignatureVerifier implements UnaryOperator<RawEnvelope> {

    public static final String METRIC = "data2flow.ingest.signature.rejected";

    private final SigningKeyCache keys;
    private final Counter missing;
    private final Counter mismatch;

    public PayloadSignatureVerifier(SigningKeyCache keys, MeterRegistry meters) {
        this.keys = keys;
        this.missing = Counter.builder(METRIC).tag("reason", "missing")
                .description("서명 키가 있는 플랫폼 브로커 기기의 서명 없는 메시지(DEVICE_SIGNATURE_INVALID)").register(meters);
        this.mismatch = Counter.builder(METRIC).tag("reason", "mismatch")
                .description("서명이 맞지 않는 플랫폼 브로커 기기 메시지(DEVICE_SIGNATURE_INVALID)").register(meters);
    }

    @Override
    public RawEnvelope apply(RawEnvelope envelope) {
        if (!SourceTypes.PLATFORM_BROKER.equals(envelope.sourceType())) {
            return envelope;
        }
        Optional<PlatformBrokerSignature.Signed> signed = PlatformBrokerSignature.parse(envelope.payload());
        Optional<SigningKeyCache.Key> key = keys.find(envelope.sourceId(), deviceKey(envelope.topic()));
        if (key.isEmpty()) {
            return envelope.withSignature(SignatureStatus.UNSIGNED,
                    signed.map(PlatformBrokerSignature.Signed::body).orElse(envelope.payload()));
        }
        if (signed.isEmpty()) {
            missing.increment();
            return envelope.withSignature(SignatureStatus.INVALID, envelope.payload());
        }
        if (!PlatformBrokerSignature.verify(key.get().signingKey(), signed.get())) {
            mismatch.increment();
            return envelope.withSignature(SignatureStatus.INVALID, envelope.payload());
        }
        return envelope.withSignature(SignatureStatus.VERIFIED, signed.get().body());
    }

    /** {@code devices/{deviceKey}/telemetry|state|…}의 deviceKey. 형식이 아니면 null(키를 찾지 못해 UNSIGNED) */
    static String deviceKey(String topic) {
        if (topic == null) {
            return null;
        }
        String[] parts = topic.split("/");
        for (int i = 0; i + 1 < parts.length; i++) {
            if ("devices".equals(parts[i]) && !parts[i + 1].isBlank()) {
                return parts[i + 1];
            }
        }
        return null;
    }
}
