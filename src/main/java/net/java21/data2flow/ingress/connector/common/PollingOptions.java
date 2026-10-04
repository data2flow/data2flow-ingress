package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.contracts.connector.PollingPolicy;

import java.time.Duration;

/**
 * 가져오기형 커넥터의 실행 옵션(DSC-09.09, PollingPolicy). 운영값은 {@link #defaults()}이고, 계약 IT는 주기를 짧게 줄인 값을 쓴다
 * (최소 주기 10초는 사용자가 정하는 폴링 주기에만 적용한다. 큐·스트림 소비의 빈 응답 대기는 주기가 아니다).
 *
 * @param minPollInterval 사용자가 정하는 폴링 주기의 하한(운영 10초)
 * @param idleInterval    큐·스트림에 가져올 것이 없을 때 다시 묻기까지(운영 1초)
 * @param writeRetryDelay 기록 실패 뒤 다시 가져오기까지
 * @param connectTimeout  연결 제한 시간
 * @param testTimeout     연결 테스트 제한 시간(BR-DSC-07 기본 15초)
 */
public record PollingOptions(Duration minPollInterval, Duration idleInterval, Duration writeRetryDelay,
                             Duration connectTimeout, Duration testTimeout) {

    public static PollingOptions defaults() {
        return new PollingOptions(PollingPolicy.MIN_INTERVAL, Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofSeconds(15));
    }

    /** 시험용: 주기 하한을 낮춘다 */
    public PollingOptions withMinPollInterval(Duration d) {
        return new PollingOptions(d, idleInterval, writeRetryDelay, connectTimeout, testTimeout);
    }

    public PollingOptions withIdleInterval(Duration d) {
        return new PollingOptions(minPollInterval, d, writeRetryDelay, connectTimeout, testTimeout);
    }

    /** 사용자가 정한 주기(초)를 하한에 맞춘다 */
    public Duration pollInterval(Duration requested) {
        return requested.compareTo(minPollInterval) < 0 ? minPollInterval : requested;
    }
}
