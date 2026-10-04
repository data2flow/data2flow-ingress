package net.java21.data2flow.ingress.connector.common;

import java.util.Map;

/**
 * 세션이 소스 지표(EVT-DSC-03 {@code counters})에 더할 누적 수(예: Webhook 서명 실패·시각 오차·재생 거부, TC-DSC-186).
 * 보고기가 1분마다 차이를 보낸다.
 */
public interface SessionCounters {

    /** 이름 → 세션을 연 뒤 누적 값 */
    Map<String, Long> counters();
}
