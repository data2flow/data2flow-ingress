package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.ingress.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.05 TC-DSC-269·273 BR-DSC-27: 토큰을 수명의 90%(늦어도 만료 60초 전)에 갱신하고, 3회 연속 실패하면 TOKEN_REFRESH_FAILED */
class RefreshingTokenTest {

    static final Instant T0 = Instant.parse("2026-10-04T00:00:00Z");

    @Test
    @DisplayName("DSC-09.05 TC-DSC-273 1시간 토큰은 54분에, 5분 토큰은 4분(만료 60초 전)에 갱신한다")
    void refreshAtNinetyPercentOrSixtySecondsBefore() {
        assertThat(RefreshingToken.refreshAt(new RefreshingToken.Token("t", T0, T0.plusSeconds(3600))))
                .isEqualTo(T0.plus(Duration.ofMinutes(54)));
        assertThat(RefreshingToken.refreshAt(new RefreshingToken.Token("t", T0, T0.plusSeconds(300))))
                .isEqualTo(T0.plusSeconds(240));
        assertThat(RefreshingToken.refreshAt(new RefreshingToken.Token("t", T0, null))).isEqualTo(Instant.MAX);
    }

    @Test
    @DisplayName("DSC-09.05 BR-DSC-27 갱신 시각 전에는 같은 토큰, 지나면 새로 받는다. 실패해도 만료 전이면 이전 토큰, 3회 연속 실패면 예외")
    void refreshAndFailures() throws Exception {
        MutableClock clock = new MutableClock(T0);
        AtomicInteger calls = new AtomicInteger();
        boolean[] fail = {false};
        RefreshingToken token = new RefreshingToken(() -> {
            calls.incrementAndGet();
            if (fail[0]) {
                throw new IllegalStateException("token endpoint down");
            }
            return new RefreshingToken.Token("tok-" + calls.get(), clock.instant(), clock.instant().plusSeconds(600));
        }, clock);
        assertThat(token.get()).isEqualTo("tok-1");
        clock.advance(Duration.ofMinutes(5));
        assertThat(token.get()).isEqualTo("tok-1");
        assertThat(calls.get()).isEqualTo(1);
        clock.advance(Duration.ofMinutes(5));   // 10분: 갱신 시각(9분) 지남, 만료(10분) 직전 아님 → 실패 시험
        fail[0] = true;
        clock.advance(Duration.ofSeconds(-30));
        assertThat(token.get()).as("만료 전이면 이전 토큰").isEqualTo("tok-1");
        assertThat(token.failures()).isEqualTo(1);
        assertThat(token.get()).isEqualTo("tok-1");
        assertThatThrownBy(token::get).isInstanceOf(RefreshingToken.TokenRefreshFailedException.class)
                .hasMessageContaining("TOKEN_REFRESH_FAILED");
        fail[0] = false;
        token.invalidate();
        assertThat(token.get()).startsWith("tok-");
        assertThat(token.failures()).isZero();
        assertThat(new RefreshingToken.Token("secret", T0, null).toString()).doesNotContain("secret");
    }
}
