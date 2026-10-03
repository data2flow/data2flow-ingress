package net.java21.data2flow.ingress.source.dto;

import net.java21.data2flow.contracts.secret.Secret;
import tools.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Objects;

/**
 * core-api 실행 설정(API-DSC-50 {@code sources[]}) 한 건을 ingress가 쓰기 좋게 정리한 것.
 *
 * @param id             데이터 소스 ID
 * @param organizationId 조직 ID
 * @param type           {@link net.java21.data2flow.contracts.message.SourceTypes} 값(정본, API-DSC-50의 {@code MQTT}는 MQTT_SUBSCRIBE로 바꿈)
 * @param lifecycle      ACTIVE 또는 PAUSED
 * @param connectorKey   커넥터 키(CONNECTOR 유형). 없으면 null
 * @param config         커넥터 설정(API의 topics·qos를 합친 것)
 * @param secrets        복호화된 비밀값(종류 → 값). 로그 금지, {@link Secret}이라 출력은 가려진다
 * @param clientIdBase   client-id 앞부분(BR-DSC-01). 없으면 null
 */
public record SourceDefinition(long id, long organizationId, String type, String lifecycle, String connectorKey,
                               JsonNode config, Map<String, Secret> secrets, String clientIdBase) {

    public static final String ACTIVE = "ACTIVE";
    public static final String PAUSED = "PAUSED";

    public SourceDefinition {
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
    }

    public boolean paused() {
        return PAUSED.equals(lifecycle);
    }

    /** 연결을 다시 맺어야 하는 변경인가(설정·비밀값·client-id). lifecycle만 바뀌면 일시정지·재개로 처리한다 */
    public boolean sameConnection(SourceDefinition other) {
        return other != null && organizationId == other.organizationId && Objects.equals(type, other.type)
                && Objects.equals(connectorKey, other.connectorKey) && Objects.equals(config, other.config)
                && secretsEqual(other.secrets) && Objects.equals(clientIdBase, other.clientIdBase);
    }

    private boolean secretsEqual(Map<String, Secret> other) {
        if (secrets.size() != other.size()) {
            return false;
        }
        for (Map.Entry<String, Secret> e : secrets.entrySet()) {
            Secret o = other.get(e.getKey());
            if (o == null || !Objects.equals(o.reveal(), e.getValue().reveal())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "SourceDefinition[id=" + id + ", org=" + organizationId + ", type=" + type + ", lifecycle=" + lifecycle
                + ", connectorKey=" + connectorKey + ", secrets=" + secrets.keySet() + "]";
    }
}
