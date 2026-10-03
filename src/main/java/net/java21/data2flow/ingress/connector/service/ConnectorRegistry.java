package net.java21.data2flow.ingress.connector.service;

import net.java21.data2flow.contracts.connector.ConnectorCatalogEntry;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ingress가 가진 커넥터 목록(connectors.md §1 ConnectorRegistry, DSC-09.02). 카탈로그 보고(EVT-DSC-09)와 소스 유형 → 커넥터 선택을 맡는다.
 * M2는 {@code mqtt} 하나이고 M5에서 커넥터 빈을 더하면 자동으로 들어온다.
 */
public class ConnectorRegistry {

    private final Map<String, SourceConnector> connectors = new LinkedHashMap<>();

    public ConnectorRegistry(List<SourceConnector> connectors) {
        connectors.forEach(c -> this.connectors.put(c.descriptor().key(), c));
    }

    public Optional<SourceConnector> find(String key) {
        return Optional.ofNullable(key == null ? null : connectors.get(key));
    }

    /**
     * 소스 유형에 맞는 커넥터 키. MQTT 구독·플랫폼 브로커는 {@code mqtt}, 커넥터 카탈로그 유형은 소스의 connectorKey.
     * ingress가 실행하지 않는 유형(SIMULATION은 simulator, WEBHOOK·EDGE·외부 맥락은 M5)은 null.
     */
    public static String connectorKeyFor(String sourceType, String connectorKey) {
        if (sourceType == null) {
            return null;
        }
        return switch (sourceType) {
            case SourceTypes.MQTT_SUBSCRIBE, SourceTypes.PLATFORM_BROKER -> "mqtt";
            case SourceTypes.CONNECTOR -> connectorKey;
            default -> null;
        };
    }

    public List<ConnectorCatalogEntry> catalog() {
        return connectors.values().stream().map(SourceConnector::catalogEntry).toList();
    }

    public List<String> keys() {
        return List.copyOf(connectors.keySet());
    }
}
