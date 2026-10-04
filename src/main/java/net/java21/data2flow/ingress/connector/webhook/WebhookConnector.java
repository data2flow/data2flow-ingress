package net.java21.data2flow.ingress.connector.webhook;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectionTestResult.Step;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.SessionCounters;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Webhook 수신 커넥터(키 {@code webhook}, 소스 유형 WEBHOOK, DSC-01.03·07.04, API-DSC-54). 외부 시스템이
 * {@code POST https://data2flow-hook.java21.net/ingest/webhook/{sourceKey}}로 보낸 본문 하나를 원본 하나로 넘긴다.
 *
 * <ul>
 *   <li><b>보안(BR-DSC-10):</b> {@code X-D2F-Signature = hex(HMAC-SHA256(HMAC_KEY, timestamp + "." + body))}, {@code X-D2F-Timestamp}
 *       ±{@code toleranceSec}(기본 300초), 같은 {@code X-D2F-Request-Id}는 10분 안에 다시 오면 409. 서명·시각이 틀리면 401이고 기록하지 않는다.</li>
 *   <li><b>무손실(AFTER_WRITE):</b> 스트림 기록 confirm 뒤에 202를 준다. 기록이 실패하거나 일시정지면 503(상대가 다시 보낸다, AT-DSC-20.3).</li>
 *   <li>본문 256KB 넘으면 413. 모든 ingress 인스턴스가 받는다(SCALABLE).</li>
 * </ul>
 * HTTP 서버는 ingress 웹 계층({@code WebhookController})이고, 이 커넥터는 {@link WebhookRouter}에 소스를 등록한다.
 */
public class WebhookConnector implements SourceConnector {

    public static final String KEY = "webhook";
    /** 본문 한도(API-DSC-54) */
    public static final int MAX_BODY_BYTES = 256 * 1024;
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final WebhookRouter router;
    private final ReplayGuard replay;
    private final Duration writeTimeout;

    public WebhookConnector(WebhookRouter router, ReplayGuard replay, Duration writeTimeout) {
        this.router = router;
        this.replay = replay;
        this.writeTimeout = writeTimeout;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "HTTP Webhook 수신", "1.0.0", ConnectorCategory.HTTP,
                Set.of(AuthMethod.TOKEN), Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.CSV),
                AckMode.AFTER_WRITE, ScalingMode.SCALABLE, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    /** 받는 쪽이므로 접속할 상대가 없다. 설정과 서명 비밀값이 있는지만 본다 */
    @Override
    public ConnectionTestResult test(SourceConfig config) {
        try {
            Settings.from(config);
            return new ConnectionTestResult(List.of(Step.skipped(ConnectionTestResult.STEP_DNS),
                    Step.skipped(ConnectionTestResult.STEP_TCP), Step.skipped(ConnectionTestResult.STEP_TLS),
                    Step.ok(ConnectionTestResult.STEP_AUTH, 0), Step.ok(ConnectionTestResult.STEP_SUBSCRIBE, 0)),
                    List.of(), false);
        } catch (InvalidSettingsException e) {
            return new ConnectionTestResult(List.of(Step.skipped(ConnectionTestResult.STEP_DNS),
                    Step.skipped(ConnectionTestResult.STEP_TCP), Step.skipped(ConnectionTestResult.STEP_TLS),
                    Step.failed(ConnectionTestResult.STEP_AUTH, 0, "SOURCE_SECRET_REQUIRED", e.field()),
                    Step.skipped(ConnectionTestResult.STEP_SUBSCRIBE)), List.of(), false);
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    /** 수신 결과(HTTP 상태와 오류 코드) */
    public record Result(int status, String code, String requestId, Instant receivedAt) {
        static Result of(int status, String code) {
            return new Result(status, code, null, null);
        }
    }

    record Settings(String sourceKey, Duration tolerance, String idHeader, String hmacKey, String topic) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            String key = c.required("sourceKey");
            if (!key.matches("[A-Za-z0-9_-]{16,64}")) {
                throw InvalidSettingsException.config("sourceKey");
            }
            return new Settings(key, c.seconds("toleranceSec", 300, 30, 3600),
                    c.text("idHeader", WebhookSignature.HEADER_REQUEST_ID), c.requiredSecret("HMAC_KEY").reveal(),
                    c.text("topic", "webhook/" + key.substring(0, 8)));
        }
    }

    /** 소스 하나의 수신 창구 */
    public final class Session implements DrainableSession, SessionCounters {
        private final SourceConfig config;
        private final Settings settings;
        private final RawSink sink;
        private final ConnectorContext ctx;
        private final AtomicLong received = new AtomicLong();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final Map<String, AtomicLong> rejected = new ConcurrentHashMap<>();
        private volatile ConnectorState state = ConnectorState.DISCONNECTED;
        private volatile boolean paused;
        private volatile Instant connectedSince;
        private volatile Instant lastReceivedAt;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            this.config = config;
            this.settings = settings;
            this.sink = sink;
            this.ctx = ctx;
        }

        @Override
        public void start() {
            router.register(settings.sourceKey(), this);
            connectedSince = ctx.clock().instant();
            state = ConnectorState.CONNECTED;
            ctx.reportStatus(status());
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
        }

        /**
         * 요청 하나를 처리한다(HTTP 스레드). 기록이 끝날 때까지 기다린다.
         *
         * @param header 요청 헤더(이름은 대소문자 무시)
         */
        public Result receive(java.util.function.Function<String, String> header, byte[] body) {
            if (state != ConnectorState.CONNECTED || paused) {
                return Result.of(503, "SOURCE_PAUSED");
            }
            if (body.length > MAX_BODY_BYTES) {
                count("tooLarge");
                return Result.of(413, "PAYLOAD_TOO_LARGE");
            }
            Instant now = ctx.clock().instant();
            String timestamp = header.apply(WebhookSignature.HEADER_TIMESTAMP);
            Instant sentAt = parseTimestamp(timestamp);
            if (sentAt == null || Duration.between(sentAt, now).abs().compareTo(settings.tolerance()) > 0) {
                count("timestampRejected");
                return Result.of(401, "WEBHOOK_TIMESTAMP_INVALID");
            }
            if (!WebhookSignature.verify(settings.hmacKey(), timestamp, body, header.apply(WebhookSignature.HEADER_SIGNATURE))) {
                count("signatureRejected");
                return Result.of(401, "WEBHOOK_SIGNATURE_INVALID");
            }
            String requestId = header.apply(settings.idHeader());
            if (requestId == null || requestId.isBlank() || requestId.length() > 128) {
                count("signatureRejected");
                return Result.of(400, "INVALID_REQUEST");
            }
            if (replay.seen(config.sourceId(), requestId, now)) {
                count("replayRejected");
                return Result.of(409, "WEBHOOK_REPLAYED");
            }
            RawEnvelope envelope = ctx.envelope(config, settings.topic(), body);
            inFlight.incrementAndGet();
            try {
                CompletableFuture<Void> write = sink.write(envelope).toCompletableFuture();
                write.get(writeTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Result.of(503, "SERVICE_UNAVAILABLE");
            } catch (Exception e) {
                count("writeFailed");
                return Result.of(503, "SERVICE_UNAVAILABLE");   // 상대가 다시 보낸다(AT-DSC-20.3)
            } finally {
                if (inFlight.decrementAndGet() == 0) {
                    synchronized (inFlight) {
                        inFlight.notifyAll();
                    }
                }
            }
            replay.remember(config.sourceId(), requestId, now);
            received.incrementAndGet();
            lastReceivedAt = now;
            return new Result(202, null, requestId, now);
        }

        private void count(String name) {
            rejected.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
        }

        @Override
        public Map<String, Long> counters() {
            Map<String, Long> out = new LinkedHashMap<>();
            rejected.forEach((k, v) -> out.put(k, v.get()));
            return out;
        }

        @Override
        public ConnectorStatus status() {
            return new ConnectorStatus(state, null, null, null, state == ConnectorState.CONNECTED ? connectedSince : null,
                    0, received.get(), lastReceivedAt);
        }

        @Override
        public boolean drainAndClose(Duration timeout) {
            paused = true;   // 새 요청은 503
            long deadline = System.nanoTime() + timeout.toNanos();
            synchronized (inFlight) {
                long left;
                while (inFlight.get() > 0 && (left = deadline - System.nanoTime()) > 0) {
                    try {
                        inFlight.wait(Math.max(1, left / 1_000_000L));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            boolean drained = inFlight.get() == 0;
            close();
            return drained;
        }

        @Override
        public void close() {
            router.unregister(settings.sourceKey(), this);
            if (state != ConnectorState.DISCONNECTED) {
                state = ConnectorState.DISCONNECTED;
                ctx.reportStatus(status());
            }
        }
    }

    /** {@code X-D2F-Timestamp}: 유닉스 초 또는 ISO-8601 */
    static Instant parseTimestamp(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String v = value.trim();
        try {
            if (v.matches("\\d{9,11}")) {
                return Instant.ofEpochSecond(Long.parseLong(v));
            }
            if (v.matches("\\d{12,14}")) {
                return Instant.ofEpochMilli(Long.parseLong(v));
            }
            return Instant.parse(v);
        } catch (DateTimeParseException | NumberFormatException e) {
            return null;
        }
    }
}
