package net.java21.data2flow.ingress.payload;

import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.07·09.08 TC-DSC-282·287 AT-DSC-16.1·16.4·16.5: 앱 전체(Testcontainers RabbitMQ Stream + Mosquitto, core 대역)에서
 * Protobuf 소스 + 토픽 템플릿 → {@code data2flow.raw}에 구조화 JSON·topicAttributes, 불일치 토픽은 UNMATCHED_TOPIC 원본,
 * 깨진 payload는 DECODE_ERROR 원본으로 기록되고 모두 확인(PUBACK)된다. 내부 API-DSC-82·83도 확인한다.
 */
class PayloadFormatIT extends AbstractIngressAppIT {

    static final long SOURCE = 77;
    static final MessageCodec CODEC = MessageCodec.create();
    static RawStreamReader reader;

    @Autowired
    SourceSupervisor supervisor;
    @Autowired
    RabbitTemplate rabbit;
    @LocalServerPort
    int port;

    @AfterAll
    static void close() {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    @DisplayName("DSC-09.07 AT-DSC-16.1 DSC-09.08 AT-DSC-16.4·16.5 Protobuf + 템플릿 소스: 변환·추출·미처리 표시가 원본 스트림에 기록된다")
    void convertsAndExtractsOnRawStream() throws Exception {
        CORE.payloadSchema("it-42", 1, "PROTOBUF", "reading.proto", PayloadTestData.PROTO.getBytes(StandardCharsets.UTF_8));
        CORE.sources("payload-1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", brokerUrl(), "site/#",
                "\"payload\":{\"format\":\"PROTOBUF\",\"schemaRef\":\"it-42\",\"messageType\":\"acme.Reading\"},"
                        + "\"topicTemplate\":\"site/{site}/room/{room}/{deviceId}/{metric}\"") + "]");
        ConfigChangedMessage msg = ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SOURCE, SOURCE, 2, 1,
                Clock.systemUTC());
        MessageProperties props = new MessageProperties();
        props.setContentType("application/json");
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "", new Message(CODEC.write(msg), props));
        await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                .anyMatch(r -> r.definition().id() == SOURCE && r.session().status().state() == ConnectorState.CONNECTED));
        reader = new RawStreamReader(RabbitTestBroker.environment());

        String ok = "site/a/room/301/em-1/temperature";
        String unmatched = "site/a/em-1";
        String broken = "site/a/room/302/em-2/humidity";
        try (MqttTestPublisher p = publisher(ok)) {
            p.publish(PayloadTestData.protobuf());
            p.publish(unmatched, PayloadTestData.protobuf());
            p.publish(broken, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> find(ok).isPresent() && find(unmatched).isPresent()
                && find(broken).isPresent());

        RawEnvelope converted = find(ok).orElseThrow();
        assertThat(PayloadTestData.normalize(converted.payload())).isEqualTo(PayloadTestData.golden());
        assertThat(converted.originalPayload()).isEqualTo(PayloadTestData.protobuf());
        assertThat(converted.payloadFormat()).isEqualTo("PROTOBUF");
        assertThat(converted.topicAttributes()).containsEntry("externalId", "em-1").containsEntry("metric", "temperature")
                .containsEntry("spaceHint", "a/301");
        MessageSchemas.assertValid(converted);

        RawEnvelope skipped = find(unmatched).orElseThrow();
        assertThat(skipped.ingressStatus()).isEqualTo(IngressStatus.UNMATCHED_TOPIC);
        assertThat(skipped.payload()).isEqualTo(PayloadTestData.protobuf());

        RawEnvelope failed = find(broken).orElseThrow();
        assertThat(failed.ingressStatus()).isEqualTo(IngressStatus.DECODE_ERROR);
        assertThat(failed.topicAttributes()).containsEntry("externalId", "em-2");
        assertThat(supervisor.running().stream().filter(r -> r.definition().id() == SOURCE).findFirst().orElseThrow()
                .counters().payloadCounters()).containsEntry("converted", 1L).containsEntry("unmatchedTopic", 1L)
                .containsEntry("decodeError", 1L);
    }

    @Test
    @DisplayName("DSC-09.07 API-DSC-82 스키마 검사(타입 목록·SOURCE_SCHEMA_INVALID 400), DSC-09.08 API-DSC-83 템플릿 미리보기")
    void internalApis() throws Exception {
        String proto = Base64.getEncoder().encodeToString(PayloadTestData.PROTO.getBytes(StandardCharsets.UTF_8));
        HttpResponse<String> ok = post("/internal/ingress/payload-schemas/inspect",
                "{\"format\":\"PROTOBUF\",\"fileName\":\"reading.proto\",\"content\":\"" + proto + "\"}");
        assertThat(ok.statusCode()).isEqualTo(200);
        JsonNode body = CODEC.mapper().readTree(ok.body()).get("response");
        assertThat(body.get("messageTypes").get(0).asString()).isEqualTo("acme.Reading");
        assertThat(body.get("messageType").asString()).isEqualTo("acme.Reading");

        HttpResponse<String> bad = post("/internal/ingress/payload-schemas/inspect", "{\"format\":\"PROTOBUF\",\"content\":\""
                + Base64.getEncoder().encodeToString("message {".getBytes(StandardCharsets.UTF_8)) + "\"}");
        assertThat(bad.statusCode()).isEqualTo(400);
        assertThat(CODEC.mapper().readTree(bad.body()).at("/header/resultCode").asString()).isEqualTo("SOURCE_SCHEMA_INVALID");

        HttpResponse<String> preview = post("/internal/ingress/topic-templates/preview",
                "{\"template\":\"site/{site}/room/{room}/{deviceId}/{metric}\",\"topics\":[\"site/a/room/301/em-1/t\",\"site/a/em-1\"]}");
        JsonNode p = CODEC.mapper().readTree(preview.body()).get("response");
        assertThat(p.get("subscriptionFilter").asString()).isEqualTo("site/+/room/+/+/+");
        assertThat(p.at("/results/0/attributes/spaceHint").asString()).isEqualTo("a/301");
        assertThat(p.at("/results/1/status").asString()).isEqualTo("UNMATCHED_TOPIC");
        HttpResponse<String> badTemplate = post("/internal/ingress/topic-templates/preview",
                "{\"template\":\"a/#/b\",\"topics\":[]}");
        assertThat(badTemplate.statusCode()).isEqualTo(400);
        assertThat(CODEC.mapper().readTree(badTemplate.body()).at("/header/resultCode").asString())
                .isEqualTo("SOURCE_CONFIG_INVALID");
    }

    HttpResponse<String> post(String path, String json) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
    }

    static Optional<RawEnvelope> find(String topic) {
        return reader.envelopes().stream().filter(e -> e.sourceId() == SOURCE && topic.equals(e.topic())).findFirst();
    }
}
