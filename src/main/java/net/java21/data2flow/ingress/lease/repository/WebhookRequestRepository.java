package net.java21.data2flow.ingress.lease.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;

/** Webhook 재생 방지 기록(ADR-052 {@code data2flow_ingress.webhook_requests}, BR-DSC-10). 인스턴스가 여럿이어도 같은 요청 ID를 막는다 */
public class WebhookRequestRepository {

    private final JdbcTemplate jdbc;

    public WebhookRequestRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean exists(long organizationId, long sourceId, String requestId, Instant since) {
        Integer n = jdbc.queryForObject("""
                SELECT count(*) FROM data2flow_ingress.webhook_requests
                 WHERE organization_id = ? AND source_id = ? AND request_id = ? AND received_at > ?
                """, Integer.class, organizationId, sourceId, requestId, Timestamp.from(since));
        return n != null && n > 0;
    }

    public void insert(long organizationId, long sourceId, String requestId, Instant at) {
        jdbc.update("""
                INSERT INTO data2flow_ingress.webhook_requests (source_id, request_id, organization_id, received_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (source_id, request_id) DO UPDATE SET received_at = EXCLUDED.received_at
                """, sourceId, requestId, organizationId, Timestamp.from(at));
    }

    @OrganizationScopeExempt("모든 조직의 10분 지난 요청 ID를 지우는 정리 작업")
    public int deleteOlderThan(Instant before) {
        return jdbc.update("DELETE FROM data2flow_ingress.webhook_requests WHERE received_at < ?", Timestamp.from(before));
    }
}
