package net.java21.data2flow.ingress.connector.domain;

import net.java21.data2flow.contracts.connector.ConnectorSession;

import java.time.Duration;

/**
 * 종료할 때 기록 중인 메시지를 마저 끝내고 닫을 수 있는 세션(reliability-and-ha.md §4.1, TC-ING-021).
 * 새 메시지는 더 받지 않고(확인하지 않아 상대가 다시 보낸다), 기록 중인 것은 confirm을 기다려 확인한 뒤 연결을 끊는다.
 */
public interface DrainableSession extends ConnectorSession {

    /**
     * @return 제한 시간 안에 기록 중인 메시지가 모두 끝났으면 true
     */
    boolean drainAndClose(Duration timeout);
}
