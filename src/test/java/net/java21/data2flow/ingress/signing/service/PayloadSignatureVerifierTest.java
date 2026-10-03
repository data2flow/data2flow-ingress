package net.java21.data2flow.ingress.signing.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.message.PlatformBrokerSignature;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SignatureStatus;
import net.java21.data2flow.contracts.message.SourceTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** DSC-03.03·03.05(BR-DSC-12, ADR-042): 플랫폼 브로커 기기 payload 서명 검증 */
class PayloadSignatureVerifierTest {

    static final byte[] BODY = "{\"co2\":812}".getBytes(StandardCharsets.UTF_8);
    final Clock clock = Clock.fixed(Instant.parse("2026-10-04T01:00:00Z"), ZoneOffset.UTC);
    final List<SigningKeyCache.Key> coreKeys = new ArrayList<>(List.of(
            new SigningKeyCache.Key(5, "esp32-co2-02", 88, 1, "key-co2-02", null),
            new SigningKeyCache.Key(5, "esp32-th-01", 89, 2, "key-th-01", null)));
    final SigningKeyCache cache = new SigningKeyCache(() -> List.copyOf(coreKeys), Duration.ofSeconds(30), clock);
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final PayloadSignatureVerifier verifier = new PayloadSignatureVerifier(cache, meters);

    RawEnvelope envelope(String sourceType, String topic, byte[] payload) {
        return RawEnvelope.of(1, 5, sourceType, topic, payload, clock.instant(), "data2flow-ingress-0", "sha256:x");
    }

    double rejected(String reason) {
        return meters.counter("data2flow.ingest.signature.rejected", "reason", reason).count();
    }

    @Test
    @DisplayName("DSC-03.03 TC-DSC-114 같은 기기의 키로 서명하면 VERIFIED이고 서명 접두사를 뗀 본문을 기록한다")
    void verified() {
        cache.refresh();
        RawEnvelope out = verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/ESP32-CO2-02/telemetry",
                PlatformBrokerSignature.sign("key-co2-02", BODY)));
        assertThat(out.signatureStatus()).isEqualTo(SignatureStatus.VERIFIED);
        assertThat(out.payload()).isEqualTo(BODY);
        assertThat(rejected("mismatch") + rejected("missing")).isZero();
    }

    @Test
    @DisplayName("DSC-03.03 TC-DSC-114 다른 기기의 서명 키로 서명하면 INVALID, 원본 그대로, 지표 mismatch 증가")
    void otherDevicesKey() {
        cache.refresh();
        byte[] signed = PlatformBrokerSignature.sign("key-th-01", BODY);
        RawEnvelope out = verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/esp32-co2-02/telemetry", signed));
        assertThat(out.signatureStatus()).isEqualTo(SignatureStatus.INVALID);
        assertThat(out.payload()).isEqualTo(signed);
        assertThat(rejected("mismatch")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("DSC-03.05 승인된(키가 있는) 기기의 서명 없는 메시지는 INVALID, 지표 missing 증가")
    void missingSignature() {
        cache.refresh();
        RawEnvelope out = verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/esp32-co2-02/telemetry", BODY));
        assertThat(out.signatureStatus()).isEqualTo(SignatureStatus.INVALID);
        assertThat(out.payload()).isEqualTo(BODY);
        assertThat(rejected("missing")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("DSC-03.05 키가 없는 기기(승인 전)는 UNSIGNED, 서명 접두사가 있으면 떼어 낸다")
    void noKeyIsUnsigned() {
        cache.refresh();
        RawEnvelope plain = verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/new-one/telemetry", BODY));
        assertThat(plain.signatureStatus()).isEqualTo(SignatureStatus.UNSIGNED);
        assertThat(plain.payload()).isEqualTo(BODY);
        RawEnvelope prefixed = verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/new-one/telemetry",
                PlatformBrokerSignature.sign("whatever", BODY)));
        assertThat(prefixed.signatureStatus()).isEqualTo(SignatureStatus.UNSIGNED);
        assertThat(prefixed.payload()).isEqualTo(BODY);
        assertThat(verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "weird", BODY)).signatureStatus())
                .isEqualTo(SignatureStatus.UNSIGNED);
        assertThat(verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, null, BODY)).signatureStatus())
                .isEqualTo(SignatureStatus.UNSIGNED);
    }

    @Test
    @DisplayName("DSC-03.02 폐기한 키는 다음 읽기 뒤 쓰이지 않아 UNSIGNED가 되고(pipeline이 승인 기기면 거부), 만료된 키도 없는 것으로 본다")
    void revokedAndExpiredKeys() {
        cache.refresh();
        byte[] signed = PlatformBrokerSignature.sign("key-co2-02", BODY);
        assertThat(verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/esp32-co2-02/telemetry", signed))
                .signatureStatus()).isEqualTo(SignatureStatus.VERIFIED);
        coreKeys.removeIf(k -> k.credentialId() == 1);
        coreKeys.add(new SigningKeyCache.Key(5, "esp32-old", 90, 3, "key-old", Instant.parse("2026-10-04T00:59:59Z")));
        assertThat(cache.refresh()).isTrue();
        assertThat(verifier.apply(envelope(SourceTypes.PLATFORM_BROKER, "devices/esp32-co2-02/telemetry", signed))
                .signatureStatus()).isEqualTo(SignatureStatus.UNSIGNED);
        assertThat(cache.find(5, "esp32-old")).isEmpty();
        assertThat(cache.find(5, null)).isEmpty();
        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("DSC-03.03 플랫폼 브로커가 아닌 소스는 건드리지 않는다(signatureStatus 없음)")
    void otherSourcesUntouched() {
        cache.refresh();
        RawEnvelope in = envelope(SourceTypes.MQTT_SUBSCRIBE, "devices/esp32-co2-02/telemetry", BODY);
        assertThat(verifier.apply(in)).isSameAs(in);
    }

    @Test
    @DisplayName("DSC-03.02 core를 읽지 못하면 이전 키를 계속 쓰고, 키 값은 toString에 나오지 않는다")
    void keepsLastGoodKeys() {
        cache.refresh();
        SigningKeyCache failing = new SigningKeyCache(() -> {
            throw new IllegalStateException("down");
        }, Duration.ofSeconds(30), clock);
        assertThat(failing.refresh()).isFalse();
        assertThat(failing.loadedAt()).isNull();
        assertThat(cache.loadedAt()).isEqualTo(clock.instant());
        assertThat(coreKeys.get(0).toString()).doesNotContain("key-co2-02");
    }
}
