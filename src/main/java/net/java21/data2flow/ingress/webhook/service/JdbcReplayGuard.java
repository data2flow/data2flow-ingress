package net.java21.data2flow.ingress.webhook.service;

import net.java21.data2flow.ingress.connector.webhook.ReplayGuard;
import net.java21.data2flow.ingress.lease.repository.WebhookRequestRepository;

import java.time.Instant;

/** 인스턴스가 여럿이어도 같은 요청 ID를 막는 재생 방지(ADR-052 {@code webhook_requests}, BR-DSC-10) */
public class JdbcReplayGuard implements ReplayGuard {

    private final WebhookRequestRepository repository;
    private volatile Instant lastPurge = Instant.EPOCH;

    public JdbcReplayGuard(WebhookRequestRepository repository) {
        this.repository = repository;
    }

    @Override
    public boolean seen(long organizationId, long sourceId, String requestId, Instant now) {
        return repository.exists(organizationId, sourceId, requestId, now.minus(WINDOW));
    }

    @Override
    public void remember(long organizationId, long sourceId, String requestId, Instant now) {
        repository.insert(organizationId, sourceId, requestId, now);
        if (now.isAfter(lastPurge.plusSeconds(60))) {   // 1분에 한 번 10분 지난 기록을 지운다
            lastPurge = now;
            purge(now);
        }
    }

    /** 10분 지난 기록 정리 */
    public int purge(Instant now) {
        return repository.deleteOlderThan(now.minus(WINDOW));
    }
}
