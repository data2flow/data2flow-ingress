package net.java21.data2flow.ingress.connector.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffTest {

    private final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 5, Duration.ofMinutes(5));

    @Test
    @DisplayName("DSC-02.02 TC-DSC-062 재연결 간격은 1, 2, 4 … 초로 늘고 60초를 넘지 않는다")
    void exponentialUpToSixtySeconds() {
        List<Long> seconds = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            seconds.add(backoff.nextDelay(false).toSeconds());
        }
        assertThat(seconds).containsExactly(1L, 2L, 4L, 8L, 16L, 32L, 60L, 60L, 60L);
        assertThat(backoff.exhausted()).isFalse();
    }

    @Test
    @DisplayName("DSC-02.01 인증·TLS 오류가 5번 이어지면 ERROR로 두고 5분마다 다시 시도한다")
    void fatalErrorsSwitchToSlowRetry() {
        for (int i = 0; i < 4; i++) {
            backoff.nextDelay(true);
        }
        assertThat(backoff.exhausted()).isFalse();
        assertThat(backoff.nextDelay(true)).isEqualTo(Duration.ofMinutes(5));
        assertThat(backoff.exhausted()).isTrue();
        assertThat(backoff.nextDelay(false)).isEqualTo(Duration.ofSeconds(32));
        assertThat(backoff.exhausted()).isFalse();
    }

    @Test
    @DisplayName("DSC-02.02 연결에 성공하면 간격이 처음(1초)으로 돌아간다")
    void resetStartsOver() {
        backoff.nextDelay(false);
        backoff.nextDelay(false);
        backoff.reset();
        assertThat(backoff.failures()).isZero();
        assertThat(backoff.nextDelay(false)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("잘못된 설정은 거부한다")
    void rejectsInvalidSettings() {
        assertThatThrownBy(() -> new Backoff(Duration.ZERO, Duration.ofSeconds(1), 1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
