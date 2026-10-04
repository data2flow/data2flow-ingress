package net.java21.data2flow.ingress.connector.webhook;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-01.03·07.04·09.02 TC-DSC-247: Webhook 수신 커넥터가 서명 클라이언트(실제 HTTP)를 상대로 계약 키트 공통 시나리오(1,000건 무손실,
 * 기록 confirm 전 2xx 금지, 기록 실패 503 → 재전송, 일시정지 503 → 재개 후 수신)를 통과하고, AT-DSC-06.1~06.3(서명·변조·재생)을 지킨다.
 * 확인 수 = 2xx 응답 수.
 */
class WebhookConnectorContractIT extends AbstractConnectorContractTest {

    static final String KEY = "kitSourceKey0123456789abcdefABCD";
    static final String HMAC = "kit-hmac-secret";
    static final WebhookRouter ROUTER = new WebhookRouter();
    static WebhookTestServer server;
    static SigningWebhookClient client;

    @BeforeAll
    static void startServer() throws Exception {
        server = new WebhookTestServer(ROUTER);
        client = new SigningWebhookClient(server.url(KEY), HMAC, Clock.systemUTC());
    }

    @AfterAll
    static void stopServer() {
        client.close();
        server.close();
    }

    @Override
    protected SourceConnector connector() {
        return new WebhookConnector(ROUTER, ReplayGuard.inMemory(), Duration.ofSeconds(30));
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("sourceKey", KEY);
        return new SourceConfig(1, 38, SourceTypes.WEBHOOK, WebhookConnector.KEY, c, Map.of("HMAC_KEY", Secret.of(HMAC)), null);
    }

    @Override
    protected ContractPeer peer() {
        return client;
    }

    @Test
    @DisplayName("DSC-07.04 AT-DSC-06.1~06.3 BR-DSC-10 올바른 서명 202·기록, 본문 1바이트 변조 401·미기록, 같은 요청 ID 두 번째 409, 5분 넘은 시각 401")
    void signatureTimestampAndReplay() throws Exception {
        startAndAwaitConnected();
        byte[] body = "{\"co2\":812}".getBytes(StandardCharsets.UTF_8);
        String ts = Long.toString(Clock.systemUTC().instant().getEpochSecond());
        String sig = WebhookSignature.sign(HMAC, ts, body);
        String id = UUID.randomUUID().toString();

        assertThat(client.sendOnce(body, id, ts, sig)).isEqualTo(202);
        await().until(() -> sink().writtenCount() == 1);
        byte[] tampered = "{\"co2\":813}".getBytes(StandardCharsets.UTF_8);
        assertThat(client.sendOnce(tampered, UUID.randomUUID().toString(), ts, sig)).isEqualTo(401);
        assertThat(client.sendOnce(body, id, ts, sig)).isEqualTo(409);
        String old = Long.toString(Clock.systemUTC().instant().minusSeconds(301).getEpochSecond());
        assertThat(client.sendOnce(body, UUID.randomUUID().toString(), old, WebhookSignature.sign(HMAC, old, body))).isEqualTo(401);
        assertThat(client.sendOnce(body, UUID.randomUUID().toString(), ts, null)).isEqualTo(401);
        assertThat(sink().writtenCount()).as("서명·재생 거부는 기록하지 않는다").isEqualTo(1);
        var counters = ((WebhookConnector.Session) session()).counters();
        assertThat(counters).containsEntry("signatureRejected", 2L).containsEntry("replayRejected", 1L)
                .containsEntry("timestampRejected", 1L);
    }

    @Test
    @DisplayName("DSC-01.03 API-DSC-54 본문 256KB 초과는 413, 없는 sourceKey는 404")
    void oversizeAndUnknownKey() throws Exception {
        startAndAwaitConnected();
        byte[] big = new byte[WebhookConnector.MAX_BODY_BYTES + 1];
        String ts = Long.toString(Clock.systemUTC().instant().getEpochSecond());
        assertThat(client.sendOnce(big, "big-1", ts, WebhookSignature.sign(HMAC, ts, big))).isEqualTo(413);
        SigningWebhookClient other = new SigningWebhookClient(server.url("unknownSourceKey0123456789"), HMAC, Clock.systemUTC());
        assertThat(other.sendOnce(big, "x", ts, "y")).isEqualTo(404);
        other.close();
    }
}
