package net.java21.data2flow.ingress.connectiontest;

import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import net.java21.data2flow.ingress.support.NginxEdge;
import net.java21.data2flow.ingress.support.TestPki;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSC-02.05·DSC-09.11 TC-DSC-303(AT-DSC-05·BR-DSC-07) API-DSC-51 {@code POST /internal/ingress/sources/test}: DNS 실패·TCP 거부·TLS 오류·
 * 인증 실패·구독 거부 각각 해당 단계만 FAILED(이후 SKIPPED), 성공하면 미리보기 최대 10건. 형식 오류 400, 조직당 동시 3개 초과 429.
 */
class StagedConnectionTestIT extends AbstractIngressAppIT {

    static final JsonMapper JSON = JsonMapper.builder().build();
    static final TestPki PKI = TestPki.create();
    static MqttAckCountingProxy wsProxy;
    static GenericContainer<?> nginx;
    static GenericContainer<?> aclBroker;

    @LocalServerPort
    int port;
    final HttpClient http = HttpClient.newHttpClient();

    @AfterAll
    static void tearDown() {
        if (nginx != null) {
            nginx.stop();
        }
        if (aclBroker != null) {
            aclBroker.stop();
        }
        if (wsProxy != null) {
            wsProxy.close();
        }
    }

    static synchronized GenericContainer<?> nginx() throws Exception {
        if (nginx == null) {
            GenericContainer<?> b = MqttTestBroker.shared();
            wsProxy = new MqttAckCountingProxy(b.getHost(), b.getMappedPort(MqttTestBroker.WS_PORT), true);
            nginx = NginxEdge.start(PKI, wsProxy.port(), 0, "iot-data", "right-password");
        }
        return nginx;
    }

    HttpResponse<String> post(String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/internal/ingress/sources/test"))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    static String request(String url, String topic, String extraConfig, String secrets, int timeoutSec) {
        return """
                {"organizationId":1,"type":"MQTT_SUBSCRIBE",
                 "config":{"url":"%s","version":"5.0"%s},
                 "topics":[{"topic":"%s","qos":1}],"secrets":%s,"timeoutSec":%d}
                """.formatted(url, extraConfig == null ? "" : "," + extraConfig, topic, secrets == null ? "{}" : secrets, timeoutSec);
    }

    static JsonNode steps(HttpResponse<String> res) {
        return JSON.readTree(res.body()).path("response").path("steps");
    }

    static String status(JsonNode steps, String name) {
        for (JsonNode s : steps) {
            if (name.equals(s.path("name").asString())) {
                return s.path("status").asString() + (s.has("code") ? ":" + s.path("code").asString() : "");
            }
        }
        return "MISSING";
    }

    @Test
    @DisplayName("DSC-02.05 TC-DSC-086 성공: 모든 단계 OK(TLS는 SKIPPED), 수신 메시지 미리보기 최대 10건, 10건이 차면 바로 끝난다")
    void successWithPreview() throws Exception {
        String topic = "preview/" + UUID.randomUUID();
        GenericContainer<?> b = MqttTestBroker.shared();
        CompletableFuture<HttpResponse<String>> call = CompletableFuture.supplyAsync(() -> {
            try {
                return post(request(brokerUrl(), topic + "/#", null, null, 20));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        try (MqttTestPublisher pub = new MqttTestPublisher(b.getHost(), b.getMappedPort(MqttTestBroker.MQTT_PORT), topic + "/x")) {
            for (int i = 0; i < 300 && !call.isDone(); i++) {
                pub.publish(("{\"i\":" + i + "}").getBytes(StandardCharsets.UTF_8));
            }
        }
        HttpResponse<String> res = call.join();
        assertThat(res.statusCode()).isEqualTo(200);
        JsonNode body = JSON.readTree(res.body());
        assertThat(body.path("header").path("isSuccessful").asBoolean()).isTrue();
        JsonNode steps = steps(res);
        assertThat(status(steps, "DNS")).isEqualTo("OK");
        assertThat(status(steps, "TCP")).isEqualTo("OK");
        assertThat(status(steps, "TLS")).isEqualTo("SKIPPED");
        assertThat(status(steps, "AUTH")).isEqualTo("OK");
        assertThat(status(steps, "SUBSCRIBE")).isEqualTo("OK");
        JsonNode preview = body.path("response").path("preview");
        assertThat(preview.size()).isEqualTo(10);
        assertThat(preview.get(0).path("topic").asString()).isEqualTo(topic + "/x");
        assertThat(preview.get(0).path("rawExcerpt").asString()).startsWith("{\"i\":");
        assertThat(body.path("response").path("lossPossible").asBoolean()).isFalse();
    }

    @Test
    @DisplayName("TC-DSC-303 DNS 실패 → DNS만 FAILED(DNS_NOT_FOUND), 나머지 SKIPPED")
    void dnsFailure() throws Exception {
        JsonNode steps = steps(post(request("tcp://no-such-host.invalid:1883", "a/#", null, null, 5)));
        assertThat(status(steps, "DNS")).isEqualTo("FAILED:DNS_NOT_FOUND");
        assertThat(status(steps, "TCP")).isEqualTo("SKIPPED");
        assertThat(status(steps, "SUBSCRIBE")).isEqualTo("SKIPPED");
    }

    @Test
    @DisplayName("TC-DSC-303 TCP 거부 → TCP만 FAILED(TCP_REFUSED)")
    void tcpRefused() throws Exception {
        int closed;
        try (ServerSocket s = new ServerSocket(0)) {
            closed = s.getLocalPort();
        }
        JsonNode steps = steps(post(request("tcp://localhost:" + closed, "a/#", null, null, 5)));
        assertThat(status(steps, "DNS")).isEqualTo("OK");
        assertThat(status(steps, "TCP")).isEqualTo("FAILED:TCP_REFUSED");
        assertThat(status(steps, "TLS")).isEqualTo("SKIPPED");
        assertThat(status(steps, "AUTH")).isEqualTo("SKIPPED");
    }

    @Test
    @DisplayName("TC-DSC-303 AT-DSC-15 TLS 인증서 체인 오류(믿지 않는 CA) → TLS만 FAILED(TLS_CERT_CHAIN)")
    void tlsChainFailure() throws Exception {
        String url = "wss://localhost:" + nginx().getMappedPort(NginxEdge.WSS_PORT) + "/mqtt";
        JsonNode steps = steps(post(request(url, "a/#", "\"auth\":\"HEADER\",\"headerScheme\":\"Basic\"",
                "{\"HEADER_VALUE\":\"iot-data:right-password\"}", 5)));
        assertThat(status(steps, "TCP")).isEqualTo("OK");
        assertThat(status(steps, "TLS")).isEqualTo("FAILED:TLS_CERT_CHAIN");
        assertThat(status(steps, "AUTH")).isEqualTo("SKIPPED");
    }

    @Test
    @DisplayName("TC-DSC-303 Basic 비밀번호가 틀리면 인증 단계만 FAILED(AUTH_REJECTED), TLS는 OK이고 인증서 체인 요약이 있다")
    void authFailure() throws Exception {
        String url = "wss://localhost:" + nginx().getMappedPort(NginxEdge.WSS_PORT) + "/mqtt";
        String secrets = "[{\"kind\":\"HEADER_VALUE\",\"value\":\"iot-data:wrong\"},{\"kind\":\"CA_CERT\",\"value\":"
                + JSON.writeValueAsString(PKI.caPem()) + "}]";
        HttpResponse<String> res = post(request(url, "a/#", "\"auth\":\"HEADER\",\"headerScheme\":\"Basic\"", secrets, 5));
        JsonNode steps = steps(res);
        assertThat(status(steps, "TLS")).isEqualTo("OK");
        assertThat(steps.findValues("tlsChain").getFirst().get(0).asString()).contains("CN=localhost");
        assertThat(status(steps, "AUTH")).isEqualTo("FAILED:AUTH_REJECTED");
        assertThat(status(steps, "SUBSCRIBE")).isEqualTo("SKIPPED");
        assertThat(res.body()).doesNotContain("wrong");
    }

    @Test
    @DisplayName("DSC-01.05 TC-DSC-239 WSS + 올바른 Basic이면 모든 단계 OK(iot-data.java21.net 방식)")
    void wssBasicSuccess() throws Exception {
        String url = "wss://localhost:" + nginx().getMappedPort(NginxEdge.WSS_PORT) + "/mqtt";
        ObjectNode secrets = JSON.createObjectNode().put("HEADER_VALUE", "iot-data:right-password").put("CA_CERT", PKI.caPem());
        JsonNode steps = steps(post(request(url, "a/#", "\"auth\":\"HEADER\",\"headerScheme\":\"Basic\"", secrets.toString(), 2)));
        for (String step : List.of("DNS", "TCP", "TLS", "AUTH", "SUBSCRIBE")) {
            assertThat(status(steps, step)).as(step).isEqualTo("OK");
        }
    }

    @Test
    @DisplayName("TC-DSC-303 브로커가 구독을 거부하면 SUBSCRIBE만 FAILED(SUBSCRIBE_REJECTED)")
    void subscribeRejected() throws Exception {
        synchronized (StagedConnectionTestIT.class) {
            if (aclBroker == null) {
                aclBroker = MqttTestBroker.createAclBroker();
                aclBroker.start();
            }
        }
        String url = "tcp://" + aclBroker.getHost() + ":" + aclBroker.getMappedPort(MqttTestBroker.MQTT_PORT);
        JsonNode steps = steps(post(request(url, "any/#", null, null, 5)));
        assertThat(status(steps, "AUTH")).isEqualTo("OK");
        assertThat(status(steps, "SUBSCRIBE")).isEqualTo("FAILED:SUBSCRIBE_REJECTED");
    }

    @Test
    @DisplayName("API-DSC-51 형식 오류는 400: 조직 없음 INVALID_REQUEST, url 틀림 SOURCE_CONFIG_INVALID, 비밀값 없음 SOURCE_SECRET_REQUIRED, 인증 방식 SOURCE_AUTH_UNSUPPORTED")
    void validationErrors() throws Exception {
        HttpResponse<String> noOrg = post("{\"type\":\"MQTT_SUBSCRIBE\",\"config\":{\"url\":\"tcp://b\"}}");
        assertThat(noOrg.statusCode()).isEqualTo(400);
        assertThat(noOrg.body()).contains("INVALID_REQUEST");
        HttpResponse<String> badUrl = post(request("http://b", "a", null, null, 3));
        assertThat(badUrl.statusCode()).isEqualTo(400);
        assertThat(badUrl.body()).contains("SOURCE_CONFIG_INVALID");
        HttpResponse<String> noSecret = post(request("tcp://b", "a", "\"auth\":\"USERPASS\",\"username\":\"u\"", null, 3));
        assertThat(noSecret.statusCode()).isEqualTo(400);
        assertThat(noSecret.body()).contains("SOURCE_SECRET_REQUIRED");
        HttpResponse<String> badAuth = post(request("tcp://b", "a", "\"auth\":\"KERBEROS\"", null, 3));
        assertThat(badAuth.statusCode()).isEqualTo(400);
        assertThat(badAuth.body()).contains("SOURCE_AUTH_UNSUPPORTED");
        HttpResponse<String> badType = post("{\"organizationId\":1,\"type\":\"KMA_WEATHER\",\"config\":{}}");
        assertThat(badType.statusCode()).isEqualTo(400);
        assertThat(badType.body()).contains("SOURCE_CONFIG_INVALID");
    }

    @Test
    @DisplayName("API-DSC-51 조직당 동시 테스트는 3개, 넘으면 429 RATE_LIMITED")
    void concurrencyLimitPerOrganization() {
        String body = request(brokerUrl(), "quiet/" + UUID.randomUUID() + "/#", null, null, 3).replace("\"organizationId\":1",
                "\"organizationId\":77");
        List<CompletableFuture<HttpResponse<String>>> calls = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            calls.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return post(body);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        List<Integer> codes = calls.stream().map(CompletableFuture::join).map(HttpResponse::statusCode).toList();
        assertThat(codes).contains(429);
        assertThat(codes.stream().filter(c -> c == 200).count()).isLessThanOrEqualTo(3).isGreaterThanOrEqualTo(1);
    }
}
