package net.java21.data2flow.ingress.lease.repository;

import net.java21.data2flow.contracts.connector.PollCursor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * SINGLETON 커넥터 리더 리스와 폴링 위치(ADR-052, {@code data2flow_ingress.connector_leases}·{@code source_poll_cursors}, BR-DSC-24·26).
 * 시각은 ingress 시계(Clock)로 정해 넘긴다(시험에서 바꿀 수 있게). 인스턴스 사이 시계 차이는 리스 길이(30초)보다 훨씬 작다고 본다(NTP).
 */
public class LeaseRepository {

    private final JdbcTemplate jdbc;

    public LeaseRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 리스를 얻는다. 아무도 없거나 만료됐으면 넘겨받고(fencing token +1), 이미 내 것이고 유효하면 같은 token으로 연장한다.
     *
     * @return 얻었으면 fencing token, 다른 인스턴스가 가졌으면 빈 값
     */
    public OptionalLong acquire(long organizationId, long sourceId, String instance, Instant now, Instant expiresAt) {
        List<Long> token = jdbc.queryForList("""
                INSERT INTO data2flow_ingress.connector_leases AS l
                    (source_id, organization_id, holder_instance, expires_at, fencing_token)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT (source_id) DO UPDATE
                   SET holder_instance = EXCLUDED.holder_instance,
                       expires_at = EXCLUDED.expires_at,
                       organization_id = EXCLUDED.organization_id,
                       fencing_token = CASE WHEN l.holder_instance = EXCLUDED.holder_instance AND l.expires_at > ?
                                            THEN l.fencing_token ELSE l.fencing_token + 1 END
                 WHERE l.holder_instance = EXCLUDED.holder_instance OR l.expires_at <= ?
                RETURNING fencing_token
                """, Long.class, sourceId, organizationId, instance, ts(expiresAt), ts(now), ts(now));
        return token.isEmpty() ? OptionalLong.empty() : OptionalLong.of(token.get(0));
    }

    /** 내 리스를 연장한다. 이미 만료됐거나 다른 인스턴스가 넘겨받았으면 false(즉시 수집 중단) */
    public boolean renew(long organizationId, long sourceId, String instance, long token, Instant now, Instant expiresAt) {
        return jdbc.update("""
                UPDATE data2flow_ingress.connector_leases SET expires_at = ?
                 WHERE organization_id = ? AND source_id = ? AND holder_instance = ? AND fencing_token = ? AND expires_at > ?
                """, ts(expiresAt), organizationId, sourceId, instance, token, ts(now)) == 1;
    }

    /** 정상 종료: 바로 넘겨받을 수 있게 만료시킨다 */
    public void release(long organizationId, long sourceId, String instance, long token) {
        jdbc.update("""
                UPDATE data2flow_ingress.connector_leases SET expires_at = to_timestamp(0)
                 WHERE organization_id = ? AND source_id = ? AND holder_instance = ? AND fencing_token = ?
                """, organizationId, sourceId, instance, token);
    }

    public Optional<PollCursor> loadCursor(long organizationId, long sourceId) {
        List<PollCursor> rows = jdbc.query("""
                SELECT cursor, page_token FROM data2flow_ingress.source_poll_cursors
                 WHERE organization_id = ? AND source_id = ?
                """, (rs, i) -> new PollCursor(rs.getString(1), rs.getString(2)), organizationId, sourceId);
        return rows.stream().findFirst();
    }

    /**
     * 위치를 저장한다. 이 인스턴스가 이 token으로 유효한 리스를 가졌을 때만, 그리고 저장된 위치의 token보다 낮지 않을 때만 쓴다.
     *
     * @return 썼으면 true. false면 리스를 잃었다(LeaseLostException)
     */
    public boolean saveCursor(long organizationId, long sourceId, String instance, long token, Instant now, PollCursor cursor) {
        return jdbc.update("""
                INSERT INTO data2flow_ingress.source_poll_cursors AS c
                    (source_id, organization_id, cursor, page_token, fencing_token, updated_at)
                SELECT ?, ?, ?, ?, ?, ?
                 WHERE EXISTS (SELECT 1 FROM data2flow_ingress.connector_leases l
                                WHERE l.organization_id = ? AND l.source_id = ? AND l.holder_instance = ?
                                  AND l.fencing_token = ? AND l.expires_at > ?)
                ON CONFLICT (source_id) DO UPDATE
                   SET cursor = EXCLUDED.cursor, page_token = EXCLUDED.page_token,
                       fencing_token = EXCLUDED.fencing_token, updated_at = EXCLUDED.updated_at
                 WHERE c.fencing_token <= EXCLUDED.fencing_token
                """, sourceId, organizationId, cursor.cursor(), cursor.pageToken(), token, ts(now),
                organizationId, sourceId, instance, token, ts(now)) == 1;
    }

    private static Timestamp ts(Instant i) {
        return Timestamp.from(i);
    }
}
