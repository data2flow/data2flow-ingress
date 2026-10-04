package net.java21.data2flow.ingress.lease.service;

import net.java21.data2flow.contracts.connector.PollCursorStore;

import java.util.Optional;

/**
 * SINGLETON 커넥터의 리더 리스와 폴링 위치 저장소(DSC-09.10, BR-DSC-24·26). 저장소가 있으면 PostgreSQL({@link JdbcLeaseManager}),
 * 없으면 이 인스턴스만 실행한다고 보는 메모리 구현({@link LocalLeaseManager}).
 */
public interface LeaseManager {

    /** 얻은 리스 */
    record Held(long organizationId, long sourceId, long fencingToken) {
    }

    /** 리스를 얻는다. 다른 인스턴스가 가졌으면 빈 값 */
    Optional<Held> tryAcquire(long organizationId, long sourceId);

    /** 연장. 잃었으면 false */
    boolean renew(Held lease);

    /** 내려놓는다(정상 종료) */
    void release(Held lease);

    /** 이 리스로만 쓸 수 있는 폴링 위치 저장소(fencing token이 낮으면 LeaseLostException) */
    PollCursorStore cursorStore(Held lease);
}
