package net.java21.data2flow.ingress.connector.common;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * 만료되는 접속 토큰(OAuth 2.0 클라이언트 자격증명, Google 서비스 계정, Azure SAS)을 들고 있다가 만료 전에 새로 받는다
 * (DSC-09.05, BR-DSC-27: 수명의 90% 시점, 늦어도 만료 60초 전. 3회 연속 실패하면 {@link TokenRefreshFailedException}
 * {@code TOKEN_REFRESH_FAILED}로 소스 ERROR). 받은 토큰은 메모리에만 두고 저장하지 않는다(DSC domain-model §6.4).
 */
public final class RefreshingToken {

    /** 연속 실패 한도 */
    public static final int MAX_FAILURES = 3;
    /** 늦어도 만료 이만큼 전에 갱신 */
    public static final Duration MIN_MARGIN = Duration.ofSeconds(60);

    /** 새 토큰을 받는다 */
    @FunctionalInterface
    public interface Fetcher {
        Token fetch() throws Exception;
    }

    /** @param expiresAt 만료 시각(없으면 만료 없음) */
    public record Token(String value, Instant issuedAt, Instant expiresAt) {
        @Override
        public String toString() {
            return "Token[***, expiresAt=" + expiresAt + "]";
        }
    }

    /** 3회 연속 갱신 실패(BR-DSC-27). 소스 상태 ERROR, 오류 코드 {@code TOKEN_REFRESH_FAILED} */
    public static class TokenRefreshFailedException extends Exception {
        public static final String CODE = "TOKEN_REFRESH_FAILED";

        public TokenRefreshFailedException(Throwable cause) {
            super(CODE + ": " + cause.getMessage(), cause);
        }
    }

    private final Fetcher fetcher;
    private final Clock clock;
    private Token current;
    private int failures;

    public RefreshingToken(Fetcher fetcher, Clock clock) {
        this.fetcher = fetcher;
        this.clock = clock;
    }

    /** 갱신할 시각: 수명의 90%와 만료 60초 전 중 빠른 쪽 */
    public static Instant refreshAt(Token t) {
        if (t.expiresAt() == null) {
            return Instant.MAX;
        }
        Duration life = Duration.between(t.issuedAt(), t.expiresAt());
        Instant ninety = t.issuedAt().plus(life.multipliedBy(9).dividedBy(10));
        Instant margin = t.expiresAt().minus(MIN_MARGIN);
        return ninety.isBefore(margin) ? ninety : margin;
    }

    /**
     * 유효한 토큰. 갱신 시각이 지났으면 새로 받는다. 받기에 실패해도 아직 만료 전이면 이전 토큰을 돌려준다.
     *
     * @throws TokenRefreshFailedException 3회 연속 실패
     * @throws Exception                   이번 시도가 실패했고 쓸 토큰이 없음
     */
    public synchronized String get() throws Exception {
        Instant now = clock.instant();
        if (current != null && now.isBefore(refreshAt(current))) {
            return current.value();
        }
        try {
            current = fetcher.fetch();
            failures = 0;
            return current.value();
        } catch (Exception e) {
            failures++;
            if (failures >= MAX_FAILURES) {
                throw new TokenRefreshFailedException(e);
            }
            if (current != null && current.expiresAt() != null && now.isBefore(current.expiresAt())) {
                return current.value();
            }
            throw e;
        }
    }

    /** 상대가 토큰을 거부했을 때(401): 다음 요청에서 새로 받는다 */
    public synchronized void invalidate() {
        current = null;
    }

    public synchronized int failures() {
        return failures;
    }
}
