package net.java21.data2flow.ingress.payload.service;

import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.ingress.payload.AvroRegistryStub;
import net.java21.data2flow.ingress.payload.PayloadTestData;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import net.java21.data2flow.ingress.payload.schema.AvroRegistryClient;
import net.java21.data2flow.ingress.payload.schema.PayloadSchemaClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

import static net.java21.data2flow.ingress.payload.PayloadTestData.golden;
import static net.java21.data2flow.ingress.payload.PayloadTestData.normalize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * DSC-09.07·09.08 TC-DSC-280·285 AT-DSC-16.1~16.5 BR-DSC-28: 기록 직전 변환·추출(core 스키마 API-DSC-81은 스텁, 레지스트리는
 * Apicurio ccompat 스텁).
 */
class PayloadTransformerTest {

    static final long ORG = 1;
    static final String TOPIC = "site/a/room/301/em-1/temperature";

    MockRestServiceServer core;
    PayloadTransformerFactory factory;
    AvroRegistryStub registry;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        core = MockRestServiceServer.bindTo(builder).ignoreExpectOrder(true).build();
        factory = new PayloadTransformerFactory(new PayloadSchemaClient(builder, PayloadTestData.JSON, "http://core"),
                new AvroRegistryClient(Duration.ofSeconds(2), PayloadTestData.JSON), 1024 * 1024, PayloadTestData.JSON);
    }

    static RawEnvelope envelope(String topic, byte[] payload) {
        return RawEnvelope.of(ORG, 5, SourceTypes.CONNECTOR, topic, payload, Instant.parse("2026-10-05T01:00:00Z"),
                "data2flow-ingress-0", DedupKeys.detect(5, topic, payload));
    }

    PayloadTransformer transformer(String config) {
        return factory.create(ORG, "mqtt", PayloadTestData.JSON.readTree(config));
    }

    void schema(String ref, long org, String format, String fileName, String content) {
        core.expect(ExpectedCount.manyTimes(), requestTo("http://core/internal/core/payload-schemas/" + ref))
                .andRespond(withSuccess("{\"schemaRef\":\"" + ref + "\",\"organizationId\":" + org + ",\"format\":\"" + format
                        + "\",\"fileName\":\"" + fileName + "\",\"content\":\""
                        + Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8)) + "\"}",
                        MediaType.APPLICATION_JSON));
    }

    @Test
    @DisplayName("DSC-09.07 AT-DSC-16.1 Protobuf 스키마 업로드·메시지 타입 Reading → 구조화된 JSON(payload), 원본은 originalPayload")
    void protobufConverted() {
        schema("42", ORG, "PROTOBUF", "reading.proto", PayloadTestData.PROTO);
        PayloadTransformer t = transformer("{\"payload\":{\"format\":\"PROTOBUF\",\"schemaRef\":\"42\",\"messageType\":\"Reading\"}}");
        byte[] original = PayloadTestData.protobuf();
        RawEnvelope in = envelope("meters/em-1", original);
        PayloadTransformer.Outcome out = t.apply(in);
        assertThat(out.counter()).isEqualTo(PayloadTransformer.CONVERTED);
        assertThat(normalize(out.envelope().payload())).isEqualTo(golden());
        assertThat(out.envelope().originalPayload()).isEqualTo(original);
        assertThat(out.envelope().payloadFormat()).isEqualTo("PROTOBUF");
        assertThat(out.envelope().dedupKey()).as("중복 키는 받은 바이트 기준 유지").isEqualTo(in.dedupKey());
        assertThat(out.envelope().messageId()).isEqualTo(in.messageId());
        assertThat(t.apply(envelope("meters/em-1", original)).counter()).as("두 번째는 캐시된 코덱").isEqualTo("converted");
        assertThat(t.settings().schemaRef()).isEqualTo("42");
    }

    @Test
    @DisplayName("DSC-09.07 AT-DSC-16.2 gzip 압축 JSON은 풀어서 그대로(payloadFormat JSON), ChirpStack deduplicationId가 있으면 그 키")
    void gzipJson() {
        byte[] json = "{\"deduplicationId\":\"0b5c1d2e-aaaa-bbbb-cccc-0123456789ab\",\"object\":{\"t\":1}}"
                .getBytes(StandardCharsets.UTF_8);
        PayloadTransformer.Outcome out = transformer("{\"payload\":{\"format\":\"json\",\"compression\":\"gzip\"}}")
                .apply(envelope("a", PayloadTestData.gzip(json)));
        assertThat(out.envelope().payload()).isEqualTo(json);
        assertThat(out.envelope().payloadFormat()).isEqualTo("JSON");
        assertThat(out.envelope().dedupKey()).isEqualTo("chirpstack:0b5c1d2e-aaaa-bbbb-cccc-0123456789ab");
        assertThat(out.envelope().ingressStatus()).isNull();
    }

    @Test
    @DisplayName("DSC-09.08 AT-DSC-16.4 템플릿으로 externalId·metric·spaceHint를 topicAttributes에 싣고 JSON은 그대로")
    void topicAttributes() {
        byte[] json = PayloadTestData.json();
        PayloadTransformer.Outcome out = transformer("{\"topicTemplate\":\"site/{site}/room/{room}/{deviceId}/{metric}\"}")
                .apply(envelope(TOPIC, json));
        assertThat(out.counter()).isNull();
        assertThat(out.envelope().payload()).isSameAs(json);
        assertThat(out.envelope().topicAttributes()).containsEntry(IngressStatus.ATTR_EXTERNAL_ID, "em-1")
                .containsEntry(IngressStatus.ATTR_METRIC, "temperature").containsEntry(IngressStatus.ATTR_SPACE_HINT, "a/301");
        assertThat(out.envelope().originalPayload()).isNull();
    }

    @Test
    @DisplayName("DSC-09.08 AT-DSC-16.5 TC-DSC-285 BR-DSC-28 템플릿에 맞지 않으면 UNMATCHED_TOPIC + 원본 그대로(변환하지 않음)")
    void unmatchedTopic() {
        byte[] cbor = PayloadTestData.cbor();
        PayloadTransformer.Outcome out = transformer("{\"payload\":{\"format\":\"cbor\"},"
                + "\"topicTemplate\":\"site/{site}/room/{room}/{deviceId}/{metric}\"}").apply(envelope("site/a/em-1", cbor));
        assertThat(out.counter()).isEqualTo(PayloadTransformer.UNMATCHED_TOPIC);
        assertThat(out.envelope().ingressStatus()).isEqualTo(IngressStatus.UNMATCHED_TOPIC);
        assertThat(out.envelope().ingressError()).contains("site/{site}");
        assertThat(out.envelope().payload()).isEqualTo(cbor);
        assertThat(out.envelope().payloadFormat()).isNull();
    }

    @Test
    @DisplayName("DSC-09.07 UC-DSC-16 1a 형식과 실제 payload가 맞지 않으면 DECODE_ERROR + 원본 그대로, 템플릿 값은 유지")
    void decodeError() {
        byte[] notCbor = "plain".getBytes(StandardCharsets.UTF_8);
        PayloadTransformer.Outcome out = transformer("{\"payload\":{\"format\":\"cbor\",\"compression\":\"deflate\"},"
                + "\"topicTemplate\":\"{deviceId}\"}").apply(envelope("d1", notCbor));
        assertThat(out.counter()).isEqualTo(PayloadTransformer.DECODE_ERROR);
        assertThat(out.envelope().ingressStatus()).isEqualTo(IngressStatus.DECODE_ERROR);
        assertThat(out.envelope().ingressError()).startsWith("CBOR+DEFLATE: ");
        assertThat(out.envelope().payload()).isEqualTo(notCbor);
        assertThat(out.envelope().topicAttributes()).containsEntry("externalId", "d1");
        assertThat(transformer("{\"payload\":{\"format\":\"msgpack\"}}").apply(envelope("x", new byte[0])).envelope()
                .ingressError()).startsWith("MSGPACK: ");
    }

    @Test
    @DisplayName("DSC-09.07 스키마 없음(404)·다른 조직·형식 다름·해석 불가·타입 없음은 DECODE_ERROR(다시 받아도 같다)")
    void schemaProblemsAreDecodeErrors() {
        core.expect(ExpectedCount.manyTimes(), requestTo("http://core/internal/core/payload-schemas/404"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));
        schema("other-org", 2, "PROTOBUF", "r.proto", PayloadTestData.PROTO);
        schema("avro-file", ORG, "AVRO", "r.avsc", PayloadTestData.AVSC);
        schema("broken", ORG, "PROTOBUF", "r.proto", "message {");
        schema("good", ORG, "PROTOBUF", "r.proto", PayloadTestData.PROTO);
        schema("bad-avsc", ORG, "AVRO", "r.avsc", "{nope");
        byte[] p = PayloadTestData.protobuf();
        assertThat(error("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"404\"}}", p)).contains("없습니다");
        assertThat(error("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"other-org\"}}", p)).contains("조직");
        assertThat(error("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"avro-file\"}}", p)).contains("AVRO 스키마");
        assertThat(error("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"broken\"}}", p)).contains("스키마를 쓸 수 없습니다");
        assertThat(error("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"good\",\"messageType\":\"Nope\"}}", p))
                .contains("Nope");
        assertThat(error("{\"payload\":{\"format\":\"avro\",\"schemaRef\":\"bad-avsc\"}}", p)).contains(".avsc");
    }

    String error(String config, byte[] payload) {
        PayloadTransformer.Outcome out = transformer(config).apply(envelope("t", payload));
        assertThat(out.envelope().ingressStatus()).isEqualTo(IngressStatus.DECODE_ERROR);
        return out.envelope().ingressError();
    }

    @Test
    @DisplayName("DSC-09.03 core 스키마 조회가 일시 실패하면 예외(기록하지 않음 → 재전송), 다음 메시지에서 다시 시도해 성공")
    void transientSchemaFailureRetries() {
        core.expect(ExpectedCount.once(), requestTo("http://core/internal/core/payload-schemas/7")).andRespond(withServerError());
        PayloadTransformer t = transformer("{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"7\"}}");
        assertThatThrownBy(() -> t.apply(envelope("t", PayloadTestData.protobuf())))
                .isInstanceOf(SchemaUnavailableException.class);
        core.reset();
        schema("7", ORG, "PROTOBUF", "reading.proto", PayloadTestData.PROTO);
        assertThat(t.apply(envelope("t", PayloadTestData.protobuf())).counter()).isEqualTo("converted");
    }

    @Test
    @DisplayName("DSC-09.07 Avro: 업로드 .avsc(본문만)와 레지스트리(Apicurio ccompat 스텁, 0x00 + ID)")
    void avro() {
        schema("avsc", ORG, "AVRO", "r.avsc", PayloadTestData.AVSC);
        RawEnvelope fixed = transformer("{\"payload\":{\"format\":\"avro\",\"schemaRef\":\"avsc\"}}")
                .apply(envelope("t", PayloadTestData.avroDatum())).envelope();
        assertThat(normalize(fixed.payload())).isEqualTo(golden());
        try (AvroRegistryStub stub = new AvroRegistryStub().avro(21, PayloadTestData.AVSC)) {
            PayloadTransformer t = transformer("{\"payload\":{\"format\":\"avro\",\"registryUrl\":\"" + stub.url() + "\"}}");
            RawEnvelope viaRegistry = t.apply(envelope("t", PayloadTestData.avroWire(21))).envelope();
            assertThat(normalize(viaRegistry.payload())).isEqualTo(golden());
            assertThat(viaRegistry.payloadFormat()).isEqualTo("AVRO");
            assertThat(t.apply(envelope("t", PayloadTestData.avroWire(22))).envelope().ingressError()).contains("ID 22");
            stub.status(23, 500);
            assertThatThrownBy(() -> t.apply(envelope("t", PayloadTestData.avroWire(23))))
                    .isInstanceOf(SchemaUnavailableException.class);
        }
    }

    @Test
    @DisplayName("DSC-09.07 CBOR·MessagePack·CSV·Sparkplug B 소스 변환, 형식 없는 소스는 손대지 않는다")
    void otherFormats() {
        assertThat(normalize(transformer("{\"payload\":{\"format\":\"cbor\"}}").apply(envelope("t", PayloadTestData.cbor()))
                .envelope().payload())).isEqualTo(golden());
        assertThat(normalize(transformer("{\"payload\":{\"format\":\"msgpack\",\"compression\":\"gzip\"}}")
                .apply(envelope("t", PayloadTestData.gzip(PayloadTestData.msgpack()))).envelope().payload())).isEqualTo(golden());
        RawEnvelope csv = transformer("{\"payload\":{\"format\":\"csv\"}}")
                .apply(envelope("t", "deviceId,temperature\nem-1,22.5".getBytes(StandardCharsets.UTF_8))).envelope();
        assertThat(PayloadTestData.JSON.readTree(csv.payload()).at("/rows/0/temperature").asDouble()).isEqualTo(22.5);
        RawEnvelope sp = factory.create(ORG, "sparkplug-b", PayloadTestData.JSON.readTree("{}"))
                .apply(envelope("spBv1.0/g/NDATA/e", PayloadTestData.sparkplug(1, 1,
                        new PayloadTestData.SpMetric("t", null, 10, 1.5)))).envelope();
        assertThat(PayloadTestData.JSON.readTree(sp.payload()).at("/metrics/0/value").asDouble()).isEqualTo(1.5);
        assertThat(sp.payloadFormat()).isEqualTo("SPARKPLUG_B");
        RawEnvelope in = envelope("t", PayloadTestData.json());
        assertThat(transformer("{}").apply(in).envelope()).isSameAs(in);
        assertThat(transformer("{\"payload\":{\"format\":\"binary\"}}").apply(in).envelope()).isSameAs(in);
    }
}
