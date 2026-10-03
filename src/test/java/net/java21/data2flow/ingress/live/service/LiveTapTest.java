package net.java21.data2flow.ingress.live.service;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.common.IngressProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LiveTapTest {

    static boolean matches(String filter, String topic) {
        return LiveTap.matches(filter, topic);
    }

    @Test
    @DisplayName("DSC-02.06 토픽 필터는 MQTT 와일드카드(+, #)로 고른다")
    void topicFilter() {
        assertThat(matches(null, "a/b")).isTrue();
        assertThat(matches("#", "a/b")).isTrue();
        assertThat(matches("application/+/device/#", "application/1/device/x/event/up")).isTrue();
        assertThat(matches("application/+/device/+/event/up", "application/1/device/x/event/up")).isTrue();
        assertThat(matches("application/+/device", "application/1/device/x")).isFalse();
        assertThat(matches("a/b", "a/c")).isFalse();
        assertThat(matches("a/b", null)).isFalse();
        assertThat(matches("a/b/c", "a/b")).isFalse();
    }

    @Test
    @DisplayName("DSC-02.06 구독 한도를 넘으면 거절(null)하고, 구독자 없는 소스 메시지는 버린다")
    void subscriberLimit() {
        LiveTap tap = new LiveTap(new IngressProperties.Live(10, 0, Duration.ofSeconds(5)));
        assertThat(tap.subscribe(1, null)).isNull();
        tap.onReceived(RawEnvelope.of(1, 1, "MQTT_SUBSCRIBE", "t", "x".getBytes(StandardCharsets.UTF_8), Instant.EPOCH, "i", "k"));
        assertThat(tap.subscriberCount()).isZero();
    }

    @Test
    @DisplayName("API-DSC-52 메시지 모양: receivedAt, topic, size, rawExcerpt(4KB 이하)")
    void view() {
        byte[] big = "x".repeat(5000).getBytes(StandardCharsets.UTF_8);
        Map<String, Object> v = LiveTap.view(RawEnvelope.of(1, 1, "MQTT_SUBSCRIBE", "t", big, Instant.EPOCH, "i", "k"));
        assertThat(v).containsEntry("topic", "t").containsEntry("size", 5000).containsEntry("receivedAt", "1970-01-01T00:00:00Z");
        assertThat((String) v.get("rawExcerpt")).hasSize(4096);
    }
}
