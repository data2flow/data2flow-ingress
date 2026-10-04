-- ingress 전용 스키마(ADR-052): 폴링 위치·SINGLETON 리더 리스·Webhook 재생 방지.
-- 쓰는 서비스가 ingress 하나라 data2flow_core가 아니라 ingress 스키마에 둔다(스키마 소유 규칙, domain-map §4). 스키마 사이 FK 없음.
-- expand 전용(추가만, ADR-030).

CREATE TABLE IF NOT EXISTS data2flow_ingress.source_poll_cursors (
    source_id bigint NOT NULL,
    organization_id bigint NOT NULL,
    cursor varchar(1024),
    page_token varchar(1024),
    fencing_token bigint NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT pk_source_poll_cursors PRIMARY KEY (source_id)
);
COMMENT ON TABLE data2flow_ingress.source_poll_cursors IS '폴링 위치(DSC-09.09). 스트림 기록 confirm 후 갱신(BR-DSC-24), 리스 fencing token이 낮은 쓰기는 거부(BR-DSC-26)';
CREATE INDEX IF NOT EXISTS ix_source_poll_cursors_organization_id_source_id
    ON data2flow_ingress.source_poll_cursors (organization_id, source_id);

CREATE TABLE IF NOT EXISTS data2flow_ingress.connector_leases (
    source_id bigint NOT NULL,
    organization_id bigint NOT NULL,
    holder_instance varchar(64) NOT NULL,
    expires_at timestamptz NOT NULL,
    fencing_token bigint NOT NULL,
    CONSTRAINT pk_connector_leases PRIMARY KEY (source_id)
);
COMMENT ON TABLE data2flow_ingress.connector_leases IS 'SINGLETON 커넥터 리더 리스(30초, 10초마다 갱신). 넘겨받을 때마다 fencing token +1(DSC-09.10)';
CREATE INDEX IF NOT EXISTS ix_connector_leases_organization_id_source_id
    ON data2flow_ingress.connector_leases (organization_id, source_id);

CREATE TABLE IF NOT EXISTS data2flow_ingress.webhook_requests (
    source_id bigint NOT NULL,
    request_id varchar(128) NOT NULL,
    organization_id bigint NOT NULL,
    received_at timestamptz NOT NULL,
    CONSTRAINT pk_webhook_requests PRIMARY KEY (source_id, request_id)
);
COMMENT ON TABLE data2flow_ingress.webhook_requests IS 'Webhook 재생 방지: 기록이 끝난 X-D2F-Request-Id(10분 보관, BR-DSC-10)';
CREATE INDEX IF NOT EXISTS ix_webhook_requests_received_at ON data2flow_ingress.webhook_requests (received_at);
