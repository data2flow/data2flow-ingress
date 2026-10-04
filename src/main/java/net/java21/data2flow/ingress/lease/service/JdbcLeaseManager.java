package net.java21.data2flow.ingress.lease.service;

import net.java21.data2flow.contracts.connector.LeaseLostException;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.PollCursorStore;
import net.java21.data2flow.ingress.lease.repository.LeaseRepository;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/** PostgreSQL 리스(ADR-052). 리스를 잃은 인스턴스의 위치 저장은 fencing token으로 거부한다(BR-DSC-26) */
public class JdbcLeaseManager implements LeaseManager {

    private final LeaseRepository repository;
    private final String instance;
    private final Duration ttl;
    private final Clock clock;

    public JdbcLeaseManager(LeaseRepository repository, String instance, Duration ttl, Clock clock) {
        this.repository = repository;
        this.instance = instance;
        this.ttl = ttl;
        this.clock = clock;
    }

    @Override
    public Optional<Held> tryAcquire(long organizationId, long sourceId) {
        Instant now = clock.instant();
        OptionalLong token = repository.acquire(organizationId, sourceId, instance, now, now.plus(ttl));
        return token.isPresent() ? Optional.of(new Held(organizationId, sourceId, token.getAsLong())) : Optional.empty();
    }

    @Override
    public boolean renew(Held lease) {
        Instant now = clock.instant();
        return repository.renew(lease.organizationId(), lease.sourceId(), instance, lease.fencingToken(), now, now.plus(ttl));
    }

    @Override
    public void release(Held lease) {
        repository.release(lease.organizationId(), lease.sourceId(), instance, lease.fencingToken());
    }

    @Override
    public PollCursorStore cursorStore(Held lease) {
        return new PollCursorStore() {
            @Override
            public Optional<PollCursor> load(long sourceId) {
                return repository.loadCursor(lease.organizationId(), sourceId);
            }

            @Override
            public void save(long sourceId, PollCursor cursor) {
                if (!repository.saveCursor(lease.organizationId(), sourceId, instance, lease.fencingToken(), clock.instant(),
                        cursor)) {
                    throw new LeaseLostException(sourceId, lease.fencingToken());
                }
            }
        };
    }
}
