package net.java21.data2flow.ingress.payload.schema;

import net.java21.data2flow.ingress.payload.PayloadTestData;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** DSC-09.07 API-DSC-81: core 업로드 스키마 조회 계약(core-api 스텁) */
class PayloadSchemaClientTest {

    static final String CONTENT = Base64.getEncoder().encodeToString(PayloadTestData.PROTO.getBytes());

    static PayloadSchemaClient client(RestClient.Builder builder) {
        return new PayloadSchemaClient(builder, PayloadTestData.JSON, "http://core");
    }

    @Test
    @DisplayName("DSC-09.07 API-DSC-81 GET /internal/core/payload-schemas/{schema-ref}를 X-CALLER-SERVICE로 읽고(봉투 포함) 영구 캐시")
    void readsEnvelopeAndCaches() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        PayloadSchemaClient client = client(builder);
        server.expect(requestTo("http://core/internal/core/payload-schemas/42")).andExpect(method(HttpMethod.GET))
                .andExpect(header("X-CALLER-SERVICE", "data2flow-ingress"))
                .andRespond(withSuccess("{\"header\":{\"isSuccessful\":true},\"response\":{\"schemaRef\":\"42\",\"organizationId\":1,"
                        + "\"sourceId\":5,\"format\":\"PROTOBUF\",\"fileName\":\"reading.proto\",\"content\":\"" + CONTENT
                        + "\",\"messageTypes\":[\"acme.Reading\"]}}", MediaType.APPLICATION_JSON));
        PayloadSchema s = client.get("42").orElseThrow();
        assertThat(s.organizationId()).isEqualTo(1);
        assertThat(s.format()).isEqualTo("PROTOBUF");
        assertThat(s.fileName()).isEqualTo("reading.proto");
        assertThat(new String(s.content())).contains("message Reading");
        assertThat(s.messageTypes()).containsExactly("acme.Reading");
        assertThat(client.get("42")).containsSame(s);
        server.verify();
    }

    @Test
    @DisplayName("DSC-09.07 404는 없음(DECODE_ERROR), 5xx·깨진 응답은 일시 오류(재전송)")
    void notFoundAndFailures() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        PayloadSchemaClient client = client(builder);
        server.expect(requestTo("http://core/internal/core/payload-schemas/1")).andRespond(withStatus(HttpStatus.NOT_FOUND));
        server.expect(requestTo("http://core/internal/core/payload-schemas/2")).andRespond(withServerError());
        server.expect(requestTo("http://core/internal/core/payload-schemas/3"))
                .andRespond(withSuccess("{\"schemaRef\":\"3\"}", MediaType.APPLICATION_JSON));
        assertThat(client.get("1")).isEmpty();
        assertThatThrownBy(() -> client.get("2")).isInstanceOf(SchemaUnavailableException.class).hasMessageContaining("500");
        assertThatThrownBy(() -> client.get("3")).isInstanceOf(SchemaUnavailableException.class).hasMessageContaining("content");
        assertThat(new PayloadSchema("x", 1, "AVRO", null, new byte[1], null).messageTypes()).isEmpty();
    }

    @Test
    @DisplayName("DSC-09.07 core에 접속할 수 없으면 일시 오류")
    void unreachable() {
        PayloadSchemaClient client = new PayloadSchemaClient(RestClient.builder()
                .requestFactory(PayloadSchemaClient.requestFactory(Duration.ofMillis(500))), PayloadTestData.JSON, "http://127.0.0.1:1");
        assertThatThrownBy(() -> client.get("9")).isInstanceOf(SchemaUnavailableException.class);
    }
}
