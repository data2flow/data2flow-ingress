package net.java21.data2flow.ingress;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.ConnectorCatalogEntry;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.message.event.ConnectorCatalogReported;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import net.java21.data2flow.ingress.connector.http.RecordsHttpServer;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.connector.webhook.WebhookSignature;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.PostgresTestDb;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * M5 커넥터 카탈로그를 앱 전체로: 저장소(PostgreSQL 18, 스키마 data2flow_ingress, Flyway migrate)가 켜진 ingress가 core 설정의 WEBHOOK 소스와
 * HTTP 폴링(CONNECTOR) 소스를 실행한다. DSC-09.01 AT-DSC-18.3 TC-DSC-319(카탈로그 보고), DSC-01.03 AT-DSC-06.1~06.3 API-DSC-54(Webhook 수신 →
 * data2flow.raw), DSC-09.09 BR-DSC-24·26(커서·리스가 DB에).
 */
class IngressConnectorCatalogIT extends AbstractIngressAppIT {

    static final long WEBHOOK_SOURCE = 60;
    static final long POLL_SOURCE = 61;
    static final String KEY = "itWebhookKey0123456789abcdefABCD";
    static final String HMAC = "it-hmac-secret";
    static final RecordsHttpServer RECORDS = new RecordsHttpServer(new InMemoryPollCursorStore(), POLL_SOURCE);
    static RawStreamReader reader;

    @Autowired
    ConnectorRegistry registry;
    @Autowired
    SourceSupervisor supervisor;
    @Autowired
    JdbcTemplate jdbc;
    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void db(DynamicPropertyRegistry registry) {
        var pg = PostgresTestDb.shared();
        registry.add("data2flow.ingress.db.url", pg::getJdbcUrl);
        registry.add("data2flow.ingress.db.username", pg::getUsername);
        registry.add("data2flow.ingress.db.password", pg::getPassword);
        registry.add("data2flow.ingress.db.flyway-mode", () -> "migrate");
        registry.add("data2flow.ingress.polling.min-interval", () -> "100ms");
        registry.add("data2flow.ingress.lease.ttl", () -> "3s");
        registry.add("data2flow.ingress.lease.renew-every", () -> "1s");
        CORE.sources("m5-1", "[" + """
                {"id":%d,"organizationId":1,"type":"WEBHOOK","lifecycle":"ACTIVE","config":{"sourceKey":"%s"},
                 "secrets":{"HMAC_KEY":"%s"}}
                """.formatted(WEBHOOK_SOURCE, KEY, HMAC) + "," + """
                {"id":%d,"organizationId":1,"type":"CONNECTOR","connectorKey":"http-poll","lifecycle":"ACTIVE",
                 "config":{"url":"%s","intervalSec":1,"itemsPath":"/items","cursorParam":"since","nextCursorPath":"/next",
                           "pageSizeParam":"limit","pageSize":50},"secrets":{}}
                """.formatted(POLL_SOURCE, RECORDS.url()) + "]");
    }

    /** 앱이 data2flow.raw를 만든 뒤에 읽기 시작한다 */
    static synchronized RawStreamReader reader() {
        if (reader == null) {
            reader = new RawStreamReader(RabbitTestBroker.environment());
        }
        return reader;
    }

    @AfterAll
    static void close() {
        if (reader != null) {
            reader.close();
        }
        RECORDS.close();
    }

    @Test
    @DisplayName("DSC-09.01 AT-DSC-18.3 TC-DSC-319 카탈로그에 M5 커넥터 22종이 키·버전·스키마·확인·확장 방식과 함께 있고 EVT-DSC-09가 스키마를 지킨다")
    void catalogContainsM5Connectors() {
        List<ConnectorCatalogEntry> catalog = registry.catalog();
        assertThat(catalog).extracting(ConnectorCatalogEntry::key).contains("mqtt", "sparkplug-b", "tts-v3", "aws-iot-core",
                "azure-iot-hub", "gcp-pubsub", "amqp091", "amqp10", "kafka", "nats-jetstream", "http-poll", "http-sse", "webhook",
                "coap", "opcua", "modbus-tcp", "bacnet-ip", "onem2m", "file-s3");
        assertThat(catalog).allSatisfy(e -> {
            assertThat(e.schema().path("type").asString()).isEqualTo("object");
            assertThat(e.version()).matches("\\d+\\.\\d+\\.\\d+");
        });
        assertThat(catalog).filteredOn(e -> e.key().equals("http-poll")).first()
                .satisfies(e -> {
                    assertThat(e.ackMode()).isEqualTo(AckMode.CURSOR);
                    assertThat(e.scaling()).isEqualTo(ScalingMode.SINGLETON);
                });
        assertThat(catalog).filteredOn(e -> e.key().equals("coap")).first()
                .satisfies(e -> assertThat(e.ackMode().lossPossible()).isTrue());
        MessageSchemas.assertValid(DomainEvent.of(EventType.CONNECTOR_CATALOG_REPORTED, 1,
                new ConnectorCatalogReported("ingress-it-0", catalog), null, Clock.systemUTC()));
    }

    @Test
    @DisplayName("DSC-01.03 AT-DSC-06.1~06.3 API-DSC-54 서명이 맞으면 202 {requestId, receivedAt}·data2flow.raw 기록, 변조 401·미기록, 같은 요청 ID 409")
    void webhookEndToEnd() throws Exception {
        await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                .anyMatch(r -> r.definition().id() == WEBHOOK_SOURCE));
        reader();
        String marker = UUID.randomUUID().toString();
        byte[] body = ("{\"marker\":\"" + marker + "\",\"co2\":812}").getBytes(StandardCharsets.UTF_8);
        String ts = Long.toString(Clock.systemUTC().instant().getEpochSecond());
        String id = UUID.randomUUID().toString();
        HttpResponse<String> ok = post(body, ts, WebhookSignature.sign(HMAC, ts, body), id);
        assertThat(ok.statusCode()).isEqualTo(202);
        assertThat(ok.body()).contains("\"isSuccessful\":true").contains("\"requestId\":\"" + id + "\"").contains("receivedAt");
        await().atMost(Duration.ofSeconds(30)).until(() -> reader().envelopes().stream()
                .anyMatch(e -> new String(e.payload(), StandardCharsets.UTF_8).contains(marker)));
        RawEnvelope e = reader().envelopes().stream().filter(x -> new String(x.payload(), StandardCharsets.UTF_8).contains(marker))
                .findFirst().orElseThrow();
        assertThat(e.sourceType()).isEqualTo(SourceTypes.WEBHOOK);
        assertThat(e.sourceId()).isEqualTo(WEBHOOK_SOURCE);
        MessageSchemas.assertValid(e);

        byte[] tampered = ("{\"marker\":\"" + marker + "-x\",\"co2\":812}").getBytes(StandardCharsets.UTF_8);
        HttpResponse<String> bad = post(tampered, ts, WebhookSignature.sign(HMAC, ts, body), UUID.randomUUID().toString());
        assertThat(bad.statusCode()).isEqualTo(401);
        assertThat(bad.body()).contains("WEBHOOK_SIGNATURE_INVALID");
        assertThat(post(body, ts, WebhookSignature.sign(HMAC, ts, body), id).statusCode()).isEqualTo(409);
        assertThat(post(body, ts, "x", "y").statusCode()).isEqualTo(401);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM data2flow_ingress.webhook_requests WHERE request_id = ?",
                Integer.class, id)).isEqualTo(1);
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).until(() -> reader().envelopes().stream()
                .noneMatch(x -> new String(x.payload(), StandardCharsets.UTF_8).contains(marker + "-x")));
    }

    @Test
    @DisplayName("DSC-09.09·09.10 BR-DSC-24·26 HTTP 폴링 소스는 리스를 얻어 실행하고 기록 뒤 위치를 data2flow_ingress에 저장한다")
    void pollingSourceUsesDbCursorAndLease() {
        reader();
        String marker = UUID.randomUUID().toString();
        RECORDS.publish(List.of(("{\"marker\":\"" + marker + "\",\"i\":1}").getBytes(StandardCharsets.UTF_8),
                ("{\"marker\":\"" + marker + "\",\"i\":2}").getBytes(StandardCharsets.UTF_8)));
        await().atMost(Duration.ofSeconds(60)).until(() -> reader().envelopes().stream()
                .filter(x -> new String(x.payload(), StandardCharsets.UTF_8).contains(marker)).count() >= 2);
        await().atMost(Duration.ofSeconds(10)).until(() -> Long.parseLong(jdbc.queryForObject(
                "SELECT cursor FROM data2flow_ingress.source_poll_cursors WHERE source_id = ?", String.class, POLL_SOURCE))
                >= RECORDS.records.size());
        Map<String, Object> lease = jdbc.queryForMap(
                "SELECT holder_instance, fencing_token FROM data2flow_ingress.connector_leases WHERE source_id = ?", POLL_SOURCE);
        assertThat(lease.get("holder_instance")).isEqualTo("ingress-it-0");
    }

    private HttpResponse<String> post(byte[] body, String ts, String sig, String id) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + "/ingest/webhook/" + KEY)).header("Content-Type", "application/json")
                .header(WebhookSignature.HEADER_TIMESTAMP, ts).header(WebhookSignature.HEADER_SIGNATURE, sig)
                .header(WebhookSignature.HEADER_REQUEST_ID, id)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
