package net.java21.data2flow.ingress.source.service;

import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * core-api 내부 API로 소스 실행 설정을 읽는다(API-DSC-50 {@code GET /internal/core/sources/runtime-config}). 토큰 없이
 * {@code X-CALLER-SERVICE: data2flow-ingress}만 붙인다(ADR-021). <b>응답에는 복호화된 비밀값이 있으므로 본문을 로그에 남기지 않는다.</b>
 *
 * <p>응답 모양은 API-DSC-50 그대로 읽되, 공통 봉투({@code {header, response}})로 와도 읽는다. 비밀값은 {@code {종류: 값}} 객체나
 * {@code [{kind, value}]} 배열 둘 다 받는다. 소스 유형 {@code MQTT}(API-DSC-50 표기)는 정본 값 MQTT_SUBSCRIBE로 바꾼다.
 */
public class CoreSourceClient {

    static final String CALLER = "data2flow-ingress";
    static final String PATH = "/internal/core/sources/runtime-config";
    private static final Logger log = LoggerFactory.getLogger(CoreSourceClient.class);

    private final RestClient rest;
    private final JsonMapper json;
    private final IngressProperties properties;

    public CoreSourceClient(RestClient.Builder builder, JsonMapper json, IngressProperties properties) {
        this.rest = builder.baseUrl(properties.coreUri()).build();
        this.json = json;
        this.properties = properties;
    }

    /**
     * @param sinceVersion 마지막으로 받은 버전. 같으면 core가 204를 준다
     * @return 바뀐 설정 전체. 바뀐 것이 없으면(204) 빈 값
     */
    public Optional<RuntimeConfigSnapshot> fetch(String sinceVersion) {
        ResponseEntity<byte[]> response = rest.get()
                .uri(b -> {
                    b.path(PATH).queryParam("lifecycle", "ACTIVE,PAUSED");
                    if (sinceVersion != null) {
                        b.queryParam("sinceVersion", sinceVersion);
                    }
                    return b.build();
                })
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .retrieve()
                .toEntity(byte[].class);
        if (response.getStatusCode().isSameCodeAs(HttpStatusCode.valueOf(204)) || response.getBody() == null
                || response.getBody().length == 0) {
            return Optional.empty();
        }
        return Optional.of(parse(json.readTree(response.getBody())));
    }

    RuntimeConfigSnapshot parse(JsonNode root) {
        JsonNode body = root.has("response") && root.get("response").isObject() ? root.get("response") : root;
        List<SourceDefinition> sources = new ArrayList<>();
        for (JsonNode s : body.path("sources")) {
            try {
                sources.add(definition(s));
            } catch (RuntimeException e) {
                log.warn("소스 {} 실행 설정을 읽을 수 없어 건너뜁니다: {}", s.path("id").asString("?"), e.getMessage());
            }
        }
        JsonNode version = body.get("version");
        return new RuntimeConfigSnapshot(version == null || version.isNull() ? null : version.asString(), sources);
    }

    private SourceDefinition definition(JsonNode s) {
        long id = s.path("id").asLong();
        long org = s.path("organizationId").asLong();
        if (id < 1 || org < 1) {
            throw new IllegalArgumentException("id·organizationId가 없습니다");
        }
        String type = s.path("type").asString("");
        if ("MQTT".equals(type)) {
            type = SourceTypes.MQTT_SUBSCRIBE;
        }
        ObjectNode config = s.path("config").isObject() ? ((ObjectNode) s.get("config")).deepCopy()
                : s.path("connection").isObject() ? ((ObjectNode) s.get("connection")).deepCopy() : json.createObjectNode();
        if (!config.has("topics") && s.path("topics").isArray() && !s.path("topics").isEmpty()) {
            config.set("topics", s.get("topics").deepCopy());
        }
        if (!config.has("qos") && s.path("qos").isNumber()) {
            config.put("qos", s.get("qos").asInt());
        }
        Map<String, Secret> secrets = secrets(s.path("secrets"));
        resolveCredentialRef(config, secrets);
        String connectorKey = s.path("connectorKey").isString() ? s.get("connectorKey").asString()
                : config.path("connector").isString() ? config.get("connector").asString() : null;
        String clientIdBase = s.path("clientId").isString() ? s.get("clientId").asString()
                : config.path("clientIdBase").isString() ? config.get("clientIdBase").asString()
                : config.path("clientIdPrefix").isString() ? config.get("clientIdPrefix").asString() : null;
        return new SourceDefinition(id, org, type, s.path("lifecycle").asString(SourceDefinition.ACTIVE), connectorKey,
                config, secrets, clientIdBase);
    }

    private static Map<String, Secret> secrets(JsonNode node) {
        Map<String, Secret> map = new LinkedHashMap<>();
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

    /**
     * connectors.md §3 {@code auth.credentialRef}: core가 비밀값을 주지 않았으면 ingress 설정(k8s Secret
     * {@code data2flow-ingress-mqtt} → {@code data2flow.ingress.credentials.*})에서 찾는다. 이름은 참조의 마지막 두 부분을
     * 하이픈으로 이은 것({@code secret://sources/iot-data/basic} → {@code iot-data-basic}).
     */
    private void resolveCredentialRef(ObjectNode config, Map<String, Secret> secrets) {
        JsonNode auth = config.path("auth");
        String ref = auth.path("credentialRef").isString() ? auth.get("credentialRef").asString()
                : config.path("credentialRef").isString() ? config.get("credentialRef").asString() : null;
        if (ref == null) {
            return;
        }
        String kind = "ws-header".equalsIgnoreCase(auth.path("type").asString("")) || "HEADER".equals(config.path("auth").asString(""))
                ? "HEADER_VALUE" : "PASSWORD";
        if (secrets.containsKey(kind)) {
            return;
        }
        String[] parts = ref.replaceFirst("^[a-z]+://", "").split("/");
        String name = parts.length >= 2 ? parts[parts.length - 2] + "-" + parts[parts.length - 1] : parts[parts.length - 1];
        String value = properties.credentials().get(name);
        if (value != null && !value.isBlank()) {
            secrets.put(kind, Secret.of(value));
        }
    }
}
