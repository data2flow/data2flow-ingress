package net.java21.data2flow.ingress.connector.webhook;

import net.java21.data2flow.contracts.test.connector.ContractPeer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 서명하는 Webhook 발신자(시험 상대). 요청마다 고유 {@code X-D2F-Request-Id}를 붙여 보내고, 2xx가 아니면 같은 요청 ID로 0.2초 뒤 다시
 * 보낸다(실제 Webhook 발신 서비스처럼). 확인 수 = 2xx 응답 수.
 */
final class SigningWebhookClient implements ContractPeer, AutoCloseable {

    private final HttpClient http = HttpClient.newBuilder().executor(Executors.newVirtualThreadPerTaskExecutor()).build();
    private final ScheduledExecutorService retries = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong rejectedResponses = new AtomicLong();
    private final URI url;
    private final String key;
    private final Clock clock;

    SigningWebhookClient(URI url, String key, Clock clock) {
        this.url = url;
        this.key = key;
        this.clock = clock;
    }

    @Override
    public void publish(List<byte[]> payloads) {
        for (byte[] p : payloads) {
            send(p, UUID.randomUUID().toString());
        }
    }

    private void send(byte[] body, String requestId) {
        String ts = Long.toString(clock.instant().getEpochSecond());
        HttpRequest req = HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .header(WebhookSignature.HEADER_TIMESTAMP, ts)
                .header(WebhookSignature.HEADER_SIGNATURE, WebhookSignature.sign(key, ts, body))
                .header(WebhookSignature.HEADER_REQUEST_ID, requestId)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        http.sendAsync(req, HttpResponse.BodyHandlers.discarding()).whenComplete((res, err) -> {
            if (err == null && res.statusCode() / 100 == 2) {
                acknowledged.incrementAndGet();
            } else {
                rejectedResponses.incrementAndGet();
                retries.schedule(() -> send(body, requestId), 200, TimeUnit.MILLISECONDS);
            }
        });
    }

    /** 한 번 보내고 상태 코드를 돌려준다(재시도 없음) */
    int sendOnce(byte[] body, String requestId, String timestamp, String signature) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (timestamp != null) {
            b.header(WebhookSignature.HEADER_TIMESTAMP, timestamp);
        }
        if (signature != null) {
            b.header(WebhookSignature.HEADER_SIGNATURE, signature);
        }
        if (requestId != null) {
            b.header(WebhookSignature.HEADER_REQUEST_ID, requestId);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    @Override
    public long acknowledgedCount() {
        return acknowledged.get();
    }

    long rejectedResponses() {
        return rejectedResponses.get();
    }

    @Override
    public void close() {
        retries.shutdownNow();
    }
}
