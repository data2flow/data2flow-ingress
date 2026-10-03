package net.java21.data2flow.ingress.connector.domain;

import java.time.Duration;

/**
 * 재연결 간격(DSC-02.02, DSC domain-model §3.2). 실패할 때마다 1, 2, 4 … 초로 늘리고 최대 60초를 넘지 않는다.
 * 인증·TLS처럼 다시 해도 실패가 확실한 오류는 {@code fatalAttempts}번 연속이면 ERROR로 두고 {@code fatalRetryInterval}(5분)마다 다시 시도한다.
 *
 * <p>상태를 가진 계산기일 뿐 시간을 재지 않는다. 기다리기는 호출하는 쪽의 스케줄러가 한다(Thread.sleep 금지).
 */
public final class Backoff {

    private final Duration initial;
    private final Duration max;
    private final int fatalAttempts;
    private final Duration fatalRetryInterval;
    private int failures;
    private int fatalFailures;

    public Backoff(Duration initial, Duration max, int fatalAttempts, Duration fatalRetryInterval) {
        if (initial.isNegative() || initial.isZero() || max.compareTo(initial) < 0 || fatalAttempts < 1) {
            throw new IllegalArgumentException("백오프 설정이 올바르지 않습니다");
        }
        this.initial = initial;
        this.max = max;
        this.fatalAttempts = fatalAttempts;
        this.fatalRetryInterval = fatalRetryInterval;
    }

    /** 실패를 기록하고 다음 시도까지 기다릴 시간을 돌려준다 */
    public synchronized Duration nextDelay(boolean fatal) {
        failures++;
        fatalFailures = fatal ? fatalFailures + 1 : 0;
        if (exhausted()) {
            return fatalRetryInterval;
        }
        int shift = Math.min(failures - 1, 30);
        long millis = initial.toMillis() << shift;
        return millis <= 0 || millis > max.toMillis() ? max : Duration.ofMillis(millis);
    }

    /** 확정 실패가 한도에 닿아 ERROR로 둘 때인가 */
    public synchronized boolean exhausted() {
        return fatalFailures >= fatalAttempts;
    }

    /** 연결에 성공하면 처음부터 */
    public synchronized void reset() {
        failures = 0;
        fatalFailures = 0;
    }

    public synchronized int failures() {
        return failures;
    }
}
