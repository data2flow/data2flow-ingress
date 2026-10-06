package net.java21.data2flow.ingress.payload.schema;

import net.java21.data2flow.ingress.payload.codec.AvroCodec;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Avro 스키마 레지스트리 조회(DSC-09.07, UC-DSC-16 2). Confluent 호환 REST {@code GET {registryUrl}/schemas/ids/{id}} →
 * {@code {"schema": "<.avsc 문자열>"}}. Apicurio Registry(Apache-2.0)는 {@code /apis/ccompat/v7}가 이 API다. 클라이언트 라이브러리 없이
 * JDK HttpClient로 부른다(Confluent 클라이언트는 Confluent Community License라 쓰지 않는다, connectors.md §2).
 *
 * <p>스키마 ID는 레지스트리에서 불변이라 (주소, ID)로 영구 캐시한다. 404는 null(→ DECODE_ERROR), 접속 실패·5xx·시간 초과는
 * {@link SchemaUnavailableException}(→ 기록 실패, 상대 재전송).
 */
public class AvroRegistryClient {

    private final HttpClient http;
    private final Duration timeout;
    private final JsonMapper json;
    private final Map<String, Schema> cache = new ConcurrentHashMap<>();

    public AvroRegistryClient(Duration timeout, JsonMapper json) {
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(timeout).followRedirects(HttpClient.Redirect.NEVER).build();
        this.timeout = timeout;
        this.json = json;
    }

    /** @return 스키마. 레지스트리에 없으면 null */
    public Schema schema(String registryUrl, int id) {
        String key = registryUrl + "#" + id;
        Schema cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        HttpResponse<byte[]> response;
        try {
            response = http.send(HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + id)).timeout(timeout)
                    .header("Accept", "application/vnd.schemaregistry.v1+json, application/json").GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new SchemaUnavailableException("스키마 레지스트리 접속 실패: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaUnavailableException("스키마 레지스트리 조회가 중단되었습니다", e);
        }
        if (response.statusCode() == 404) {
            return null;
        }
        if (response.statusCode() / 100 != 2) {
            throw new SchemaUnavailableException("스키마 레지스트리 응답 HTTP " + response.statusCode(), null);
        }
        Schema schema;
        try {
            JsonNode root = json.readTree(response.body());
            String type = root.path("schemaType").asString("AVRO");
            if (!"AVRO".equalsIgnoreCase(type)) {
                return null;   // Protobuf·JSON Schema는 Avro 소스에서 쓸 수 없다
            }
            schema = AvroCodec.parseSchema(root.path("schema").asString());
        } catch (SchemaParseException | tools.jackson.core.JacksonException e) {
            throw new SchemaInvalidException("레지스트리의 스키마 " + id + "를 해석할 수 없습니다: " + e.getMessage(), e);
        }
        cache.put(key, schema);
        return schema;
    }
}
