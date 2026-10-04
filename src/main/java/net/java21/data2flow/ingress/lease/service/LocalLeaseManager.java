package net.java21.data2flow.ingress.lease.service;

import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.PollCursorStore;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 저장소가 없을 때(로컬 개발): 이 인스턴스가 늘 리더이고 폴링 위치는 메모리에만 둔다. 재시작하면 처음부터 다시 읽고(중복은 pipeline이 거름),
 * 인스턴스를 둘 띄우면 둘 다 폴링한다. 운영·staging은 반드시 저장소를 쓴다(ADR-052).
 */
public class LocalLeaseManager implements LeaseManager {

    private final Map<Long, PollCursor> cursors = new ConcurrentHashMap<>();

    @Override
    public Optional<Held> tryAcquire(long organizationId, long sourceId) {
        return Optional.of(new Held(organizationId, sourceId, 1));
    }

    @Override
    public boolean renew(Held lease) {
        return true;
    }

    @Override
    public void release(Held lease) {
        // 메모리 리스
    }

    @Override
    public PollCursorStore cursorStore(Held lease) {
        return new PollCursorStore() {
            @Override
            public Optional<PollCursor> load(long sourceId) {
                return Optional.ofNullable(cursors.get(sourceId));
            }

            @Override
            public void save(long sourceId, PollCursor cursor) {
                cursors.put(sourceId, cursor);
            }
        };
    }
}
