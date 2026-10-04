package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.LeaseLostException;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.domain.Backoff;
import net.java21.data2flow.ingress.connector.domain.ConnectionErrors;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 가져오기(pull)형 커넥터 세션의 공통 뼈대(connectors.md §1.1·1.2, DSC-02.01·02.02·09.03). 가상 스레드 하나가
 * {@code connect → (pollOnce)* → disconnect}를 돌고, 실패하면 백오프(1, 2, 4 … 최대 60초, 인증·TLS 5회면 ERROR)로 다시 맺는다.
 *
 * <p><b>무손실 규칙:</b> 하위 클래스는 {@link #writeAll}이 정상으로 돌아온 뒤에만 상대에게 확인(ack·커밋·커서 저장)한다.
 * {@link WriteFailedException}이면 확인하지 않고(nack·되감기) 잠시 뒤 다시 가져온다. {@link LeaseLostException}(리스 상실, BR-DSC-26)이면
 * 즉시 멈춘다.
 *
 * <p>일시정지 동안은 {@link #pollOnce}를 부르지 않는다(상대에 쌓인다). 상대가 오래 안 부르면 끊는 프로토콜(Kafka)은 {@link #pausedTick}을 쓴다.
 */
public abstract class LoopSession implements DrainableSession {

    private static final Logger log = LoggerFactory.getLogger(LoopSession.class);

    protected final SourceConfig config;
    protected final RawSink sink;
    protected final ConnectorContext ctx;
    private final Backoff backoff;
    private final Duration idleInterval;
    private final Duration writeRetryDelay;
    private final Semaphore wake = new Semaphore(0);
    private final AtomicLong received = new AtomicLong();
    private final Object lifecycle = new Object();
    private volatile boolean running;
    private volatile boolean paused;
    private volatile boolean stopRequested;
    private volatile ConnectorState state = ConnectorState.DISCONNECTED;
    private volatile ConnectionErrorKind errorKind;
    private volatile String errorMessage;
    private volatile Instant connectedSince;
    private volatile Instant lastReceivedAt;
    private volatile int reconnects;
    private volatile Thread worker;

    /**
     * @param idleInterval    가져올 것이 없을 때 다음 시도까지 기다리는 시간(폴링 주기)
     * @param writeRetryDelay 기록 실패 뒤 다시 가져오기까지
     */
    protected LoopSession(SourceConfig config, RawSink sink, ConnectorContext ctx, Backoff backoff,
                          Duration idleInterval, Duration writeRetryDelay) {
        this.config = config;
        this.sink = sink;
        this.ctx = ctx;
        this.backoff = backoff;
        this.idleInterval = idleInterval;
        this.writeRetryDelay = writeRetryDelay;
    }

    /** 표준 백오프(DSC-02.02: 1초부터 최대 60초, 확정 실패 5회면 ERROR 후 5분마다) */
    public static Backoff standardBackoff() {
        return new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 5, Duration.ofMinutes(5));
    }

    // ---- 하위 클래스 구현 ----

    /** 상대와 연결하고 구독·소비를 준비한다 */
    protected abstract void connect() throws Exception;

    /**
     * 한 번 가져와서 기록하고 확인한다.
     *
     * @return 처리한 것이 있으면 true(바로 다시 부른다), 없으면 false({@code idleInterval} 기다림)
     */
    protected abstract boolean pollOnce() throws Exception;

    /** 연결을 끊는다(작업 스레드에서 부른다). 예외를 던지지 않는다 */
    protected abstract void disconnect();

    /** 일시정지 중 주기적으로 부른다(연결 유지가 필요한 프로토콜). 기본은 아무것도 하지 않는다 */
    protected void pausedTick() throws Exception {
    }

    /** close가 작업 스레드를 깨울 때(블로킹 호출 중단: Kafka {@code wakeup()} 등). 기본은 인터럽트 */
    protected void interruptClient(Thread worker) {
        worker.interrupt();
    }

    /** 상태 보고에 실을 client-id(없으면 null) */
    protected String clientId() {
        return config.clientId();
    }

    // ---- 공통 도우미 ----

    /** 원본 봉투를 만든다(내용 해시 중복 키) */
    protected RawEnvelope envelope(String topic, byte[] payload) {
        return ctx.envelope(config, topic, payload);
    }

    /**
     * 모두 기록하고 confirm을 기다린다. 하나라도 실패하면 {@link WriteFailedException}. 확인(ack·커밋·커서)은 이 메서드가 정상으로 돌아온 뒤에만
     * 한다(BR-DSC-24).
     */
    protected void writeAll(List<RawEnvelope> envelopes) throws WriteFailedException, InterruptedException {
        if (envelopes.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(envelopes.size());
        for (RawEnvelope e : envelopes) {
            futures.add(sink.write(e).toCompletableFuture());
        }
        Throwable failure = null;
        for (CompletableFuture<Void> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                failure = failure == null ? e.getCause() : failure;
            }
        }
        if (failure != null) {
            throw new WriteFailedException(failure);
        }
        received.addAndGet(envelopes.size());
        lastReceivedAt = ctx.clock().instant();
    }

    /** 정해진 시간만큼(또는 깨울 때까지) 기다린다. Thread.sleep 대신 세마포어 */
    protected void await(Duration d) throws InterruptedException {
        if (d.isZero() || d.isNegative()) {
            return;
        }
        if (wake.tryAcquire(d.toMillis(), TimeUnit.MILLISECONDS)) {
            wake.drainPermits();
        }
    }

    protected boolean isRunning() {
        return running && !stopRequested;
    }

    protected boolean isPaused() {
        return paused;
    }

    // ---- 수명 ----

    @Override
    public void start() {
        synchronized (lifecycle) {
            if (running) {
                return;
            }
            running = true;
            stopRequested = false;
            state = ConnectorState.CONNECTING;
            worker = Thread.ofVirtual().name("connector-" + config.connectorKey() + "-" + config.sourceId()).start(this::run);
        }
    }

    private void run() {
        try {
            while (isRunning()) {
                boolean connected = false;
                try {
                    setState(ConnectorState.CONNECTING, null, null);
                    connect();
                    connected = true;
                    backoff.reset();
                    connectedSince = ctx.clock().instant();
                    setState(ConnectorState.CONNECTED, null, null);
                    loop();
                } catch (LeaseLostException e) {
                    log.warn("소스 {}: 리스를 잃어 수집을 멈춥니다(fencing token {})", config.sourceId(), e.fencingToken());
                    stopRequested = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    if (!isRunning()) {
                        return;
                    }
                    ConnectionErrorKind kind = ConnectionErrors.classify(e);
                    boolean fatal = ConnectionErrors.isFatal(kind);
                    Duration delay = backoff.nextDelay(fatal);
                    if (connected) {
                        reconnects++;
                    }
                    String message = ConnectionErrors.describe(e);
                    setState(backoff.exhausted() ? ConnectorState.ERROR : ConnectorState.DISCONNECTED,
                            backoff.exhausted() ? kind : kind, message);
                    log.info("소스 {}({}) 연결 실패, {} 뒤 다시 시도: {}", config.sourceId(), config.connectorKey(), delay, message);
                    disconnectQuietly();
                    connectedSince = null;
                    await(delay);
                    continue;
                } finally {
                    if (!isRunning()) {
                        disconnectQuietly();
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            disconnectQuietly();
            connectedSince = null;
            running = false;
            setState(ConnectorState.DISCONNECTED, null, null);
        }
    }

    private void loop() throws Exception {
        while (isRunning()) {
            if (paused) {
                pausedTick();
                await(Duration.ofMillis(200));
                continue;
            }
            boolean worked;
            try {
                worked = pollOnce();
            } catch (WriteFailedException e) {
                log.info("소스 {}: 기록 실패, 확인하지 않고 {} 뒤 다시 가져옵니다: {}", config.sourceId(), writeRetryDelay,
                        e.getMessage());
                await(writeRetryDelay);
                continue;
            }
            if (!worked) {
                await(idleInterval);
            }
        }
    }

    private void disconnectQuietly() {
        try {
            disconnect();
        } catch (RuntimeException e) {
            log.debug("소스 {} 연결 해제 중 오류: {}", config.sourceId(), e.toString());
        }
    }

    private void setState(ConnectorState s, ConnectionErrorKind kind, String message) {
        ConnectorState previous = state;
        state = s;
        errorKind = s == ConnectorState.ERROR ? (kind == null ? ConnectionErrorKind.OTHER : kind) : kind;
        errorMessage = message;
        if (previous != s || s == ConnectorState.ERROR) {
            ctx.reportStatus(status());
        }
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
        wake.release();
    }

    @Override
    public ConnectorStatus status() {
        ConnectorState s = state;
        ConnectionErrorKind kind = s == ConnectorState.ERROR && errorKind == null ? ConnectionErrorKind.OTHER : errorKind;
        return new ConnectorStatus(s, kind, errorMessage, clientId(),
                s == ConnectorState.CONNECTED ? connectedSince : null, reconnects, received.get(), lastReceivedAt);
    }

    @Override
    public boolean drainAndClose(Duration timeout) {
        Thread w;
        synchronized (lifecycle) {
            stopRequested = true;
            w = worker;
        }
        wake.release();
        boolean drained = true;
        if (w != null && w != Thread.currentThread()) {
            try {
                drained = w.join(timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                drained = false;
            }
        }
        close();
        return drained;
    }

    @Override
    public void close() {
        Thread w;
        synchronized (lifecycle) {
            stopRequested = true;
            w = worker;
        }
        wake.release();
        if (w != null && w.isAlive() && w != Thread.currentThread()) {
            interruptClient(w);
            try {
                if (!w.join(Duration.ofSeconds(5))) {
                    w.interrupt();
                    w.join(Duration.ofSeconds(5));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (w == null) {
            disconnectQuietly();
        }
        running = false;
        if (state != ConnectorState.DISCONNECTED) {
            setState(ConnectorState.DISCONNECTED, null, null);
        }
    }
}
