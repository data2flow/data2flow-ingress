package net.java21.data2flow.ingress.payload.schema;

import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 업로드한 payload 스키마를 core에서 읽는다(API-DSC-81 {@code GET /internal/core/payload-schemas/{schema-ref}}, 토큰 없이
 * {@code X-CALLER-SERVICE: data2flow-ingress}, ADR-021). 응답은 공통 봉투({@code {header, response}})로 와도 읽는다.
 * <pre>{@code
 * {"schemaRef":"42","organizationId":1,"sourceId":5,"format":"PROTOBUF","fileName":"reading.proto",
 *  "content":"<base64>","messageTypes":["acme.Reading"]}
 * }</pre>
 * 참조는 불변이라 한 번 읽으면 프로세스가 끝날 때까지 캐시한다. 404는 "없음"(빈 값 → DECODE_ERROR), 그 밖의 실패는
 * {@link SchemaUnavailableException}(기록 실패 → 상대 재전송).
 */
public class PayloadSchemaClient {

    static final String PATH = "/internal/core/payload-schemas/{schemaRef}";
    private static final String CALLER = "data2flow-ingress";

    private final RestClient rest;
    private final JsonMapper json;
    private final Map<String, PayloadSchema> cache = new ConcurrentHashMap<>();

    /** 제한 시간을 둔 요청 팩토리(운영 빈에서 builder에 건다) */
    public static SimpleClientHttpRequestFactory requestFactory(Duration timeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeout);
        factory.setReadTimeout(timeout);
        return factory;
    }

    public PayloadSchemaClient(RestClient.Builder builder, JsonMapper json, String coreUri) {
        this.rest = builder.baseUrl(coreUri).build();
        this.json = json;
    }

    public Optional<PayloadSchema> get(String schemaRef) {
        PayloadSchema cached = cache.get(schemaRef);
        if (cached != null) {
            return Optional.of(cached);
        }
        byte[] body;
        try {
            body = rest.get().uri(PATH, schemaRef).header(DataflowHeaders.CALLER_SERVICE, CALLER).retrieve().body(byte[].class);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().isSameCodeAs(HttpStatusCode.valueOf(404))) {
                return Optional.empty();
            }
            throw new SchemaUnavailableException("core 스키마 조회 실패(HTTP " + e.getStatusCode().value() + ")", e);
        } catch (RestClientException e) {
            throw new SchemaUnavailableException("core 스키마 조회 실패: " + e.getMessage(), e);
        }
        PayloadSchema schema = parse(body);
        cache.put(schemaRef, schema);
        return Optional.of(schema);
    }

    PayloadSchema parse(byte[] body) {
        try {
            JsonNode root = json.readTree(body);
            JsonNode r = root.has("response") && root.get("response").isObject() ? root.get("response") : root;
            List<String> types = new ArrayList<>();
            r.path("messageTypes").forEach(t -> types.add(t.asString()));
            String content = r.path("content").asString("");
            if (content.isEmpty()) {
                throw new IllegalArgumentException("content가 없습니다");
            }
            return new PayloadSchema(r.path("schemaRef").asString(), r.path("organizationId").asLong(),
                    r.path("format").asString(""), r.path("fileName").asString(null), Base64.getDecoder().decode(content), types);
        } catch (RuntimeException e) {
            throw new SchemaUnavailableException("core 스키마 응답을 읽을 수 없습니다: " + e.getMessage(), e);
        }
    }
}
