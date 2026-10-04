package net.java21.data2flow.ingress.connector.webhook;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Webhook 재생 방지(BR-DSC-10: 같은 {@code X-D2F-Request-Id}가 10분 안에 다시 오면 거부). 기록이 끝난(202) 요청 ID만 기억한다.
 * 기록 전에 기억하면 기록이 실패한 요청을 상대가 다시 보낼 때 409로 막혀 유실된다.
 */
public interface ReplayGuard {

    /** 기억하는 시간 */
    Duration WINDOW = Duration.ofMinutes(10);

    /** 이미 처리한 요청 ID인가 */
    boolean seen(long sourceId, String requestId, Instant now);

    /** 처리한 요청 ID를 기억한다 */
    void remember(long sourceId, String requestId, Instant now);

    /** 인스턴스 메모리(DB가 없을 때, 단일 인스턴스) */
    static ReplayGuard inMemory() {
        return new InMemory();
    }

    /** 메모리 구현 */
    final class InMemory implements ReplayGuard {
        private final Map<String, Instant> seen = new ConcurrentHashMap<>();

        @Override
        public boolean seen(long sourceId, String requestId, Instant now) {
            Instant at = seen.get(sourceId + ":" + requestId);
            return at != null && at.plus(WINDOW).isAfter(now);
        }

        @Override
        public void remember(long sourceId, String requestId, Instant now) {
            if (seen.size() > 100_000) {
                seen.values().removeIf(at -> at.plus(WINDOW).isBefore(now));
            }
            seen.put(sourceId + ":" + requestId, now);
        }
    }
}
