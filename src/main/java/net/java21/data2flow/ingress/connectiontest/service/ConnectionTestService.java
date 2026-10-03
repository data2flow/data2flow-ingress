package net.java21.data2flow.ingress.connectiontest.service;

import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.common.IngressErrorCode;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connectiontest.dto.SourceTestRequest;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * 저장 전 연결 테스트(DSC-02.05·09.11, API-DSC-51, BR-DSC-07). 커넥터의 {@link SourceConnector#test}를 부르고 결과를 그대로 돌려준다.
 *
 * <ul>
 *   <li>제한 시간: 요청 {@code timeoutSec}(1~30초, 기본 15초). 결과는 저장하지 않는다.</li>
 *   <li>조직당 동시 테스트 3개. 넘으면 429 {@code RATE_LIMITED}.</li>
 *   <li>구독만 한다. 테스트 client-id는 {@code {base}-test-{난수}}, clean start.</li>
 * </ul>
 */
public class ConnectionTestService {

    private final ConnectorRegistry registry;
    private final IngressProperties.ConnectionTest config;
    private final Map<Long, Semaphore> perOrganization = new ConcurrentHashMap<>();

    public ConnectionTestService(ConnectorRegistry registry, IngressProperties.ConnectionTest config) {
        this.registry = registry;
        this.config = config;
    }

    public ConnectionTestResult test(SourceTestRequest request) {
        String type = "MQTT".equals(request.type()) ? SourceTypes.MQTT_SUBSCRIBE : request.type();
        String key = ConnectorRegistry.connectorKeyFor(type, request.connectorKey());
        SourceConnector connector = registry.find(key).orElseThrow(
                () -> new BusinessException(IngressErrorCode.SOURCE_CONFIG_INVALID, "type"));
        if (!request.config().isObject()) {
            throw new BusinessException(IngressErrorCode.SOURCE_CONFIG_INVALID, "config");
        }
        ObjectNode cfg = ((ObjectNode) request.config()).deepCopy();
        if (!cfg.has("topics") && request.topics() != null && request.topics().isArray()) {
            cfg.set("topics", request.topics().deepCopy());
        }
        cfg.put("testTimeoutSec", timeout(request.timeoutSec()).toSeconds());
        SourceConfig source = new SourceConfig(request.organizationId(), 1, type, key, cfg, secrets(request.secrets()), null);

        Semaphore slots = perOrganization.computeIfAbsent(request.organizationId(),
                id -> new Semaphore(config.maxPerOrganization()));
        if (!slots.tryAcquire()) {
            throw new BusinessException(CommonErrorCode.RATE_LIMITED, 5).withHeader("Retry-After", "5");
        }
        try {
            return connector.test(source);
        } catch (InvalidSettingsException e) {
            throw switch (e.reason()) {
                case SECRET_REQUIRED -> new BusinessException(IngressErrorCode.SOURCE_SECRET_REQUIRED);
                case AUTH_UNSUPPORTED -> new BusinessException(IngressErrorCode.SOURCE_AUTH_UNSUPPORTED);
                default -> new BusinessException(IngressErrorCode.SOURCE_CONFIG_INVALID, e.field());
            };
        } finally {
            slots.release();
        }
    }

    Duration timeout(Integer seconds) {
        if (seconds == null || seconds < 1) {
            return config.defaultTimeout();
        }
        Duration requested = Duration.ofSeconds(seconds);
        return requested.compareTo(config.maxTimeout()) > 0 ? config.maxTimeout() : requested;
    }

    static Map<String, Secret> secrets(JsonNode node) {
        Map<String, Secret> map = new LinkedHashMap<>();
        if (node == null || node.isNull()) {
            return map;
        }
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> e : node.properties()) {
                if (e.getValue().isValueNode() && !e.getValue().isNull()) {
                    map.put(e.getKey(), Secret.of(e.getValue().asString()));
                }
            }
        } else if (node.isArray()) {
            for (JsonNode item : node) {
                if (item.path("kind").isString() && item.path("value").isValueNode()) {
                    map.put(item.get("kind").asString(), Secret.of(item.get("value").asString()));
                }
            }
        }
        return map;
    }
}
