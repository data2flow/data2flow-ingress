package net.java21.data2flow.ingress.connector.webhook;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** DSC-07.04 TC-DSC-181 BR-DSC-10: hex(HMAC-SHA256(key, timestamp + "." + body)), 상수 시간 비교, 시각 형식 */
class WebhookSignatureTest {

    @Test
    @DisplayName("DSC-07.04 BR-DSC-10 서명은 소문자 hex 64자, sha256= 접두사 허용, 1바이트 변조·다른 시각은 불일치")
    void signAndVerify() {
        byte[] body = "{\"co2\":812}".getBytes(StandardCharsets.UTF_8);
        String sig = WebhookSignature.sign("key", "1790000000", body);
        assertThat(sig).matches("[0-9a-f]{64}");
        assertThat(WebhookSignature.verify("key", "1790000000", body, sig)).isTrue();
        assertThat(WebhookSignature.verify("key", "1790000000", body, "sha256=" + sig.toUpperCase())).isTrue();
        assertThat(WebhookSignature.verify("key", "1790000001", body, sig)).isFalse();
        assertThat(WebhookSignature.verify("key", "1790000000", "{\"co2\":813}".getBytes(StandardCharsets.UTF_8), sig)).isFalse();
        assertThat(WebhookSignature.verify("key", null, body, sig)).isFalse();
        assertThat(WebhookSignature.verify("key", "1790000000", body, null)).isFalse();
    }

    @Test
    @DisplayName("DSC-07.04 X-D2F-Timestamp: 유닉스 초·밀리초·ISO-8601, 그 밖은 거부")
    void timestamps() {
        assertThat(WebhookConnector.parseTimestamp("1790000000")).isEqualTo(Instant.ofEpochSecond(1790000000));
        assertThat(WebhookConnector.parseTimestamp("1790000000123")).isEqualTo(Instant.ofEpochMilli(1790000000123L));
        assertThat(WebhookConnector.parseTimestamp("2026-10-04T00:00:00Z")).isEqualTo(Instant.parse("2026-10-04T00:00:00Z"));
        assertThat(WebhookConnector.parseTimestamp("yesterday")).isNull();
        assertThat(WebhookConnector.parseTimestamp(" ")).isNull();
        assertThat(WebhookConnector.parseTimestamp(null)).isNull();
    }
}
