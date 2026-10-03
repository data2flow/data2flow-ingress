package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.domain.AckSequencer;
import net.java21.data2flow.ingress.connector.domain.Backoff;
import net.java21.data2flow.ingress.connector.domain.ConnectionErrors;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQTT 소스 하나의 구독 세션(DSC-09.02·09.03·02.01·02.02, reliability-and-ha.md ①②).
 *
 * <ul>
 *   <li><b>확인 시점:</b> QoS 1·2 메시지는 {@link RawSink#write}가 정상 완료(스트림 publisher confirm)된 뒤에만, 받은 순서대로 PUBACK한다
 *       ({@link AckSequencer}).</li>
 *   <li><b>기록 실패·confirm 시간 초과:</b> 이 연결에서는 더 확인하지 않고 연결을 끊은 뒤 영속 세션으로 다시 접속한다. 브로커는 확인받지 못한
 *       메시지를 다시 보낸다(MQTT는 재접속할 때만 재전송).</li>
 *   <li><b>재연결:</b> 끊기면 1, 2, 4 … 최대 60초 간격으로 다시 접속하고 구독을 복원한다. 인증·TLS 오류가 5번 이어지면 ERROR로 두고 5분마다
 *       시도한다.</li>
 *   <li><b>역압:</b> MQTT 5는 receive maximum으로 미확인 메시지 수를 제한하고, 3.1.1은 브로커의 in-flight 한도가 같은 일을 한다. 기록이
 *       밀리면 확인이 늦어지고 브로커가 보내기를 멈춘다.</li>
 *   <li><b>일시정지(PAUSED):</b> 세션을 남긴 채 끊는다. 재개하면 같은 client-id로 접속해 쌓인 메시지부터 받는다(DSC-07.01).</li>
 *   <li><b>종료:</b> {@link #drainAndClose}는 새 메시지를 넘기지 않고 기록 중인 것의 confirm·확인까지 기다린 뒤 끊는다(TC-ING-021).</li>
 * </ul>
 */
final class MqttConnectorSession implements DrainableSession {

    private static final Logger log = LoggerFactory.getLogger(MqttConnectorSession.class);

    private final SourceConfig source;
    private final MqttSourceSettings settings;
    private final RawSink sink;
    private final ConnectorContext ctx;
    private final MqttConnectorOptions options;
    private final ScheduledExecutorService scheduler;
    private final String clientId;
    private final ExecutorService callbacks;
    /** 닫은 뒤 늦게 오는 클라이언트 콜백(세션 종료 알림 등)은 조용히 버린다 */
    private final java.util.concurrent.Executor callbackExecutor;
    private final Backoff backoff;
    private final Backoff writeBackoff;
    private final Object lock = new Object();

    private Connection current;
    private ScheduledFuture<?> pendingReconnect;
    private volatile boolean running;
    private volatile boolean paused;
    private volatile boolean draining;
    private volatile boolean closed;
    private volatile ConnectorState state = ConnectorState.DISCONNECTED;
    private volatile ConnectionErrorKind errorKind;
    private volatile String errorMessage;
    private volatile Instant connectedSince;
    private volatile Instant lastReceivedAt;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong writeFailures = new AtomicLong();
    private final Deque<Instant> reconnects = new ArrayDeque<>();

    /** 연결 한 번. 다시 접속하면 새 연결이고, 이전 연결의 늦은 콜백은 무시한다 */
    private final class Connection {
        final MqttLink link;
        final AckSequencer acks = new AckSequencer();
        volatile boolean alive = true;

        Connection() {
            this.link = new MqttLink(settings, clientId, options.connectTimeout().toMillis(),
                    incoming -> onMessage(this, incoming), cause -> onConnectionLost(this, cause), callbackExecutor);
        }
    }

    MqttConnectorSession(SourceConfig source, MqttSourceSettings settings, RawSink sink, ConnectorContext ctx,
                         MqttConnectorOptions options, ScheduledExecutorService scheduler, String clientId) {
        this.source = source;
        this.settings = settings;
        this.sink = sink;
        this.ctx = ctx;
        this.options = options;
        this.scheduler = scheduler;
        this.clientId = clientId;
        this.callbacks = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("mqtt-source-" + source.sourceId()).factory());
        this.callbackExecutor = command -> {
            try {
                callbacks.execute(command);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                log.trace("닫힌 세션의 콜백을 버립니다");
            }
        };
        this.backoff = new Backoff(options.backoffInitial(), options.backoffMax(), options.fatalAttempts(),
                options.fatalRetryInterval());
        this.writeBackoff = new Backoff(options.backoffInitial(), options.backoffMax(), Integer.MAX_VALUE,
                options.backoffMax());
    }

    @Override
    public void start() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("닫힌 세션입니다: source " + source.sourceId());
            }
            if (running) {
                return;
            }
            running = true;
            if (paused) {
                setState(ConnectorState.DISABLED, null, null);
                return;
            }
            connectLocked();
        }
    }

    @Override
    public void pause() {
        synchronized (lock) {
            paused = true;
            cancelReconnectLocked();
            dropCurrentLocked();
            if (running) {
                setState(ConnectorState.DISABLED, null, null);
            }
        }
    }

    @Override
    public void resume() {
        synchronized (lock) {
            paused = false;
            if (running && !closed && current == null) {
                connectLocked();
            }
        }
    }

    @Override
    public ConnectorStatus status() {
        ConnectorState s = state;
        ConnectionErrorKind kind = errorKind;
        if (s == ConnectorState.ERROR && kind == null) {
            kind = ConnectionErrorKind.OTHER;
        }
        return new ConnectorStatus(s, kind, errorMessage, clientId,
                s == ConnectorState.CONNECTED ? connectedSince : null, reconnects24h(), received.get(), lastReceivedAt);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            running = false;
            cancelReconnectLocked();
            dropCurrentLocked();
            setState(ConnectorState.DISCONNECTED, null, null);
        }
        callbacks.shutdown();
    }

    @Override
    public boolean drainAndClose(Duration timeout) {
        draining = true;
        Connection c;
        synchronized (lock) {
            c = current;
        }
        boolean idle = true;
        if (c != null) {
            try {
                idle = c.acks.drain(timeout.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                idle = false;
            }
        }
        close();
        return idle;
    }

    /** 이 세션이 보낸 확인(PUBACK) 수(지표) */
    long acknowledged() {
        return acknowledged.get();
    }

    /** 기록 중인 메시지 수(지표·graceful shutdown) */
    int inFlight() {
        Connection c = current;
        return c == null ? 0 : c.acks.inFlight();
    }

    String clientId() {
        return clientId;
    }

    // ---- 연결 ----

    private void connectLocked() {
        pendingReconnect = null;
        Connection c = new Connection();
        current = c;
        setState(ConnectorState.CONNECTING, errorKind, errorMessage);
        CompletableFuture<Void> f;
        try {
            f = c.link.connect(settings.cleanStart(), settings.sessionExpirySec(), options.receiveMaximum())
                    .thenCompose(v -> c.link.subscribe());
        } catch (RuntimeException e) {
            f = CompletableFuture.failedFuture(e);
        }
        f.whenComplete((v, error) -> {
            if (error == null) {
                onConnected(c);
            } else {
                onConnectionLost(c, error);
            }
        });
    }

    private void onConnected(Connection c) {
        synchronized (lock) {
            if (c != current) {
                c.link.disconnect();
                return;
            }
            backoff.reset();
            connectedSince = ctx.clock().instant();
            setState(ConnectorState.CONNECTED, null, null);
            log.info("MQTT 소스 {} 연결됨(client-id {}, {})", source.sourceId(), clientId, settings);
        }
    }

    private void onConnectionLost(Connection c, Throwable cause) {
        synchronized (lock) {
            if (c != current || !running) {
                return;
            }
            current = null;
            c.alive = false;
            c.link.disconnect();
            ConnectionErrorKind kind = ConnectionErrors.classify(cause);
            Duration delay = backoff.nextDelay(ConnectionErrors.isFatal(kind));
            recordReconnectLocked();
            String message = ConnectionErrors.describe(cause);
            setState(backoff.exhausted() ? ConnectorState.ERROR : ConnectorState.DISCONNECTED, kind, message);
            log.warn("MQTT 소스 {} 연결 끊김({}: {}), {}ms 뒤 다시 접속", source.sourceId(), kind, message, delay.toMillis());
            scheduleReconnectLocked(delay);
        }
    }

    /** 기록 실패·confirm 시간 초과: 확인하지 않은 채 끊고 영속 세션으로 다시 접속해 재전송을 받는다(reliability-and-ha.md ②) */
    private void onWriteFailed(Connection c, Throwable cause) {
        writeFailures.incrementAndGet();
        synchronized (lock) {
            if (c != current || !running) {
                return;
            }
            current = null;
            c.alive = false;
            c.link.disconnect();
            Duration delay = writeBackoff.nextDelay(false);
            recordReconnectLocked();
            String message = "스트림 기록 실패: " + ConnectionErrors.describe(cause);
            setState(ConnectorState.DISCONNECTED, ConnectionErrorKind.OTHER, message);
            log.warn("MQTT 소스 {} {} → 확인하지 않고 {}ms 뒤 다시 접속(브로커 재전송)", source.sourceId(), message, delay.toMillis());
            scheduleReconnectLocked(delay);
        }
    }

    private void scheduleReconnectLocked(Duration delay) {
        if (paused || closed) {
            return;
        }
        cancelReconnectLocked();
        pendingReconnect = scheduler.schedule(() -> {
            synchronized (lock) {
                if (running && !paused && !closed && current == null) {
                    connectLocked();
                }
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cancelReconnectLocked() {
        if (pendingReconnect != null) {
            pendingReconnect.cancel(false);
            pendingReconnect = null;
        }
    }

    private void dropCurrentLocked() {
        Connection c = current;
        current = null;
        if (c != null) {
            c.alive = false;
            c.link.disconnect();
        }
    }

    // ---- 수신 ----

    private void onMessage(Connection c, MqttLink.Incoming in) {
        if (c != current || !c.alive || paused || draining || !running) {
            return;   // 확인하지 않는다 → 다음 연결에서 브로커가 다시 보낸다
        }
        byte[] payload = in.payload();
        if (payload.length > options.maxPayloadBytes()) {
            payload = Arrays.copyOf(payload, options.maxPayloadBytes());
        }
        long seq = c.acks.register(() -> {
            try {
                in.ack().run();
                acknowledged.incrementAndGet();
            } catch (RuntimeException e) {
                log.debug("MQTT 확인 실패(이미 끊긴 연결): {}", e.toString());
            }
        });
        CompletionStage<Void> write;
        try {
            RawEnvelope envelope = ctx.envelope(source, in.topic(), payload);
            write = sink.write(envelope);
        } catch (RuntimeException e) {
            write = CompletableFuture.failedFuture(e);
        }
        write.toCompletableFuture()
                .orTimeout(options.confirmTimeout().toMillis() + 1000, TimeUnit.MILLISECONDS)
                .whenComplete((ok, error) -> {
                    if (error == null) {
                        received.incrementAndGet();
                        lastReceivedAt = ctx.clock().instant();
                        writeBackoff.reset();
                        c.acks.complete(seq);
                    } else {
                        c.acks.fail(seq);
                        onWriteFailed(c, error);
                    }
                });
    }

    // ---- 상태 ----

    private void setState(ConnectorState newState, ConnectionErrorKind kind, String message) {
        boolean changed = state != newState || errorKind != kind;
        state = newState;
        errorKind = kind;
        errorMessage = message;
        if (changed) {
            ctx.reportStatus(status());
        }
    }

    private void recordReconnectLocked() {
        Instant now = ctx.clock().instant();
        synchronized (reconnects) {
            reconnects.addLast(now);
        }
        pruneReconnects(now);
    }

    private int reconnects24h() {
        synchronized (reconnects) {
            pruneReconnects(ctx.clock().instant());
            return reconnects.size();
        }
    }

    private void pruneReconnects(Instant now) {
        synchronized (reconnects) {
            Instant from = now.minus(Duration.ofHours(24));
            while (!reconnects.isEmpty() && reconnects.peekFirst().isBefore(from)) {
                reconnects.pollFirst();
            }
        }
    }
}
