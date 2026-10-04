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
 * M2는 {@code mqtt} 하나였고 M5에서 카탈로그 커넥터(DSC-09)를 빈으로 더했다.
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
     * 소스 유형에 맞는 커넥터 키. MQTT 구독·플랫폼 브로커는 {@code mqtt}, 커넥터 카탈로그 유형은 소스의 connectorKey, WEBHOOK·OPCUA·
     * ONEM2M·MODBUS_TCP는 같은 이름의 커넥터. ingress가 실행하지 않는 유형(SIMULATION은 simulator, EDGE·외부 맥락)은 null.
     */
    public static String connectorKeyFor(String sourceType, String connectorKey) {
        if (sourceType == null) {
            return null;
        }
        return switch (sourceType) {
            case SourceTypes.MQTT_SUBSCRIBE, SourceTypes.PLATFORM_BROKER -> "mqtt";
            case SourceTypes.CONNECTOR -> connectorKey;
            case SourceTypes.WEBHOOK -> "webhook";
            case SourceTypes.OPCUA -> "opcua";
            case SourceTypes.ONEM2M -> "onem2m";
            case SourceTypes.MODBUS_TCP -> "modbus-tcp";
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
