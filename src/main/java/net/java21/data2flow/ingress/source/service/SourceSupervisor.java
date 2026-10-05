package net.java21.data2flow.ingress.source.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import net.java21.data2flow.ingress.payload.service.PayloadTransformer;
import net.java21.data2flow.ingress.payload.service.PayloadTransformerFactory;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.ClientIds;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.lease.service.LeaseManager;
import net.java21.data2flow.ingress.lease.service.LocalLeaseManager;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

/**
 * 소스별 커넥터 세션을 실행·관리한다(connectors.md §1 ConnectorRuntime, BR-DSC-05, DSC-07.01).
 *
 * <ul>
 *   <li>core 설정(API-DSC-50)을 받으면 차이만 반영한다: 새 소스는 열고, 연결 설정·비밀값·client-id가 바뀐 소스만 다시 맺고(다른 소스 영향
 *       없음, BR-DSC-05), lifecycle만 바뀌면 일시정지·재개, 목록에서 빠지면 닫는다.</li>
 *   <li>client-id: {@code {base}-{env}-{n}}(운영 {@code data2flow-ingress-prod-1}, 개발자 {@code …-dev-{이름}-{n}}, BR-DSC-01).</li>
 *   <li>종료: 다른 빈보다 먼저 멈추며 모든 세션을 동시에 drain(최대 20초)한 뒤 스트림 생산자가 닫힌다(reliability-and-ha.md §4.1).</li>
 *   <li>SINGLETON 커넥터(폴링·OPC UA·CoAP·SSE)는 리더 리스를 얻은 인스턴스만 연다. 못 얻으면 대기하고 리스 주기마다 다시 시도한다. 리스를
 *       잃으면 즉시 닫는다(DSC-09.10, BR-DSC-26). 폴링 위치는 리스에 묶인 저장소에 쓴다.</li>
 * </ul>
 */
public class SourceSupervisor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SourceSupervisor.class);
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 100;

    /** 상태 변화·통계를 받는 곳(EVT-DSC-02·03) */
    public interface StatusListener {
        void onStatus(SourceDefinition source, ConnectorStatus status);
    }

    /** 수신 기록 직후 알림(실시간 원본 보기 API-DSC-52) */
    public interface ReceivedListener {
        void onReceived(RawEnvelope envelope);
    }

    /**
     * 실행 중인 소스 하나
     *
     * @param lease SINGLETON 커넥터의 리더 리스(BR-DSC-26). 그 밖은 null
     */
    public record Running(SourceDefinition definition, SourceConfig config, ConnectorSession session,
                          SourceCounters counters, LeaseManager.Held lease) {
        public Running(SourceDefinition definition, SourceConfig config, ConnectorSession session, SourceCounters counters) {
            this(definition, config, session, counters, null);
        }
    }

    /** 소스별 누적 통계(EVT-DSC-03 원천) */
    public static final class SourceCounters {
        final AtomicLong received = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        /** payload 변환 결과(DSC-09.07·09.08): converted·decodeError·unmatchedTopic → 누적 수 */
        final Map<String, AtomicLong> payload = new java.util.concurrent.ConcurrentHashMap<>();

        /** payload 변환 결과 누적 수(이름 → 값). EVT-DSC-03 {@code counters}에 1분 차이로 싣는다 */
        public Map<String, Long> payloadCounters() {
            Map<String, Long> copy = new java.util.TreeMap<>();
            payload.forEach((k, v) -> copy.put(k, v.get()));
            return copy;
        }

        public long received() {
            return received.get();
        }

        public long bytes() {
            return bytes.get();
        }
    }

    private final IngressProperties properties;
    private final ConnectorRegistry registry;
    private final RawSink writer;
    private final StatusListener statusListener;
    private final ReceivedListener receivedListener;
    private final MeterRegistry meters;
    private final Clock clock;
    private final UnaryOperator<RawEnvelope> envelopeFilter;
    private final Map<Long, Running> running = new java.util.concurrent.ConcurrentHashMap<>();
    /** 리스를 얻지 못해 기다리는 SINGLETON 소스(다른 인스턴스가 리더, DSC-09.10) */
    private final Map<Long, SourceDefinition> standby = new java.util.concurrent.ConcurrentHashMap<>();
    private LeaseManager leases = new LocalLeaseManager();
    private volatile net.java21.data2flow.ingress.downlink.service.ChirpStackAckRouter downlinkAcks;
    private volatile PayloadTransformerFactory payloadTransformers;
    private java.util.concurrent.ScheduledExecutorService leaseTimer;
    private final Set<Long> warnedSkipped = new HashSet<>();
    private volatile boolean started;

    public SourceSupervisor(IngressProperties properties, ConnectorRegistry registry, RawSink writer,
                            StatusListener statusListener, ReceivedListener receivedListener, MeterRegistry meters,
                            Clock clock) {
        this(properties, registry, writer, statusListener, receivedListener, meters, clock, UnaryOperator.identity());
    }

    /**
     * @param envelopeFilter 스트림에 기록하기 전에 봉투를 바꾼다(플랫폼 브로커 서명 검증, DSC-03.03). 같은 스레드에서 바로 실행되어
     *                       confirm 뒤 PUBACK 순서는 그대로다
     */
    public SourceSupervisor(IngressProperties properties, ConnectorRegistry registry, RawSink writer,
                            StatusListener statusListener, ReceivedListener receivedListener, MeterRegistry meters,
                            Clock clock, UnaryOperator<RawEnvelope> envelopeFilter) {
        this.envelopeFilter = envelopeFilter;
        this.properties = properties;
        this.registry = registry;
        this.writer = writer;
        this.statusListener = statusListener;
        this.receivedListener = receivedListener;
        this.meters = meters;
        this.clock = clock;
        Gauge.builder("data2flow.ingress.sources.running", running, Map::size)
                .description("이 인스턴스가 실행 중인 소스 세션 수").register(meters);
    }

    /** SINGLETON 커넥터 리스·폴링 위치 저장소(ADR-052). 기본은 메모리(인스턴스 하나) */
    public void useLeaseManager(LeaseManager leases) {
        this.leases = leases;
    }

    /** ChirpStack 다운링크 결과(event/ack·txack)를 원본 스트림 대신 EVT-ACT-09로 낸다(ACT-03.03). 없으면 모두 원본으로 기록한다 */
    public void useDownlinkAckRouter(net.java21.data2flow.ingress.downlink.service.ChirpStackAckRouter router) {
        this.downlinkAcks = router;
    }

    /** payload 형식 변환·토픽 템플릿(DSC-09.07·09.08). 없으면 받은 그대로 기록한다 */
    public void usePayloadTransformers(PayloadTransformerFactory factory) {
        this.payloadTransformers = factory;
    }

    /** core 설정을 반영한다. 같은 설정이 다시 와도 아무것도 하지 않는다(멱등) */
    public synchronized void apply(RuntimeConfigSnapshot snapshot) {
        if (!started) {
            return;
        }
        Map<Long, SourceDefinition> wanted = new LinkedHashMap<>();
        for (SourceDefinition d : snapshot.sources()) {
            if (accepts(d)) {
                wanted.put(d.id(), d);
            }
        }
        for (Long id : new ArrayList<>(running.keySet())) {
            if (!wanted.containsKey(id)) {
                Running r = running.remove(id);
                log.info("소스 {} 실행 종료(설정 목록에서 빠짐)", id);
                drain(r);
            }
        }
        standby.keySet().removeIf(id -> !wanted.containsKey(id));
        for (SourceDefinition d : wanted.values()) {
            if (standby.containsKey(d.id())) {
                standby.put(d.id(), d);   // 리더가 아니면 최신 설정만 기억한다
                continue;
            }
            Running r = running.get(d.id());
            if (r != null && r.definition().sameConnection(d)) {
                if (r.definition().paused() != d.paused()) {
                    if (d.paused()) {
                        r.session().pause();
                    } else {
                        r.session().resume();
                    }
                    log.info("소스 {} {}", d.id(), d.paused() ? "일시정지(세션 유지)" : "재개");
                }
                running.put(d.id(), new Running(d, r.config(), r.session(), r.counters(), r.lease()));
                continue;
            }
            if (r != null) {
                log.info("소스 {} 연결 설정이 바뀌어 다시 맺습니다", d.id());
                running.remove(d.id());
                drain(r);
            }
            open(d, r == null ? new SourceCounters() : r.counters());
        }
    }

    private boolean accepts(SourceDefinition d) {
        String key = ConnectorRegistry.connectorKeyFor(d.type(), d.connectorKey());
        if (key == null) {
            return false;   // simulator·Webhook·외부 맥락 등 ingress가 구독하지 않는 유형
        }
        IngressProperties.SourceFilter filter = properties.sourceFilter();
        if (!filter.organizationIds().isEmpty() && !filter.organizationIds().contains(d.organizationId())) {
            return false;
        }
        String host = host(d);
        if (host != null && filter.deniedHosts().stream().anyMatch(h -> h.equalsIgnoreCase(host))) {
            if (warnedSkipped.add(d.id())) {
                log.warn("소스 {}의 브로커 {}는 이 환경에서 구독하지 않습니다(source-filter.denied-hosts)", d.id(), host);
            }
            return false;
        }
        return true;
    }

    /** 접속 대상 호스트(source-filter.denied-hosts 비교). 커넥터마다 주소 필드 이름이 다르다 */
    static String host(SourceDefinition d) {
        for (String field : new String[]{"url", "endpointUrl", "cseUrl", "endpoint"}) {
            String url = d.config().path(field).asString(null);
            if (url != null) {
                try {
                    String h = URI.create(url.trim()).getHost();
                    return h == null ? null : h.toLowerCase(Locale.ROOT);
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        }
        String host = d.config().path("host").asString(null);
        return host == null ? null : host.toLowerCase(Locale.ROOT);
    }

    private void open(SourceDefinition d, SourceCounters counters) {
        String key = ConnectorRegistry.connectorKeyFor(d.type(), d.connectorKey());
        SourceConnector connector = registry.find(key).orElse(null);
        if (connector == null) {
            log.warn("소스 {}: 커넥터 {}가 이 ingress에 없습니다(CONNECTOR_UNAVAILABLE)", d.id(), key);
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.OTHER, "CONNECTOR_UNAVAILABLE: " + key));
            return;
        }
        LeaseManager.Held lease = null;
        if (connector.descriptor().scaling() == ScalingMode.SINGLETON) {
            lease = leases.tryAcquire(d.organizationId(), d.id()).orElse(null);
            if (lease == null) {
                if (standby.put(d.id(), d) == null) {
                    log.info("소스 {}: 다른 인스턴스가 리더라 대기합니다(SINGLETON, BR-DSC-26)", d.id());
                    statusListener.onStatus(d, new ConnectorStatus(ConnectorState.DISCONNECTED, null,
                            "STANDBY: 다른 ingress 인스턴스가 리더입니다", null, null, 0, 0, null));
                }
                return;
            }
            standby.remove(d.id());
        }
        PayloadTransformer transformer;
        try {
            PayloadTransformerFactory factory = payloadTransformers;
            transformer = factory == null ? null : factory.create(d.organizationId(), key, d.config());
        } catch (IllegalArgumentException e) {
            releaseQuietly(lease);
            log.warn("소스 {} payload 설정 오류: {}", d.id(), e.getMessage());
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.PROTOCOL,
                    "SOURCE_CONFIG_INVALID: " + e.getMessage()));
            return;
        }
        SourceConfig config = new SourceConfig(d.organizationId(), d.id(), d.type(), key, d.config(), d.secrets(),
                clientId(d));
        Counter receivedMeter = Counter.builder("data2flow.ingress.messages.received")
                .tag("sourceId", Long.toString(d.id())).description("스트림 기록까지 끝난 수신 메시지 수(DSC-02.03)")
                .register(meters);
        RawSink sink = received -> {
            var acks = downlinkAcks;
            if (acks != null && acks.matches(received)) {
                // 텔레메트리가 아니다: 이벤트 발행 confirm 뒤에 확인한다(원본 스트림·수신 통계에는 넣지 않음)
                return acks.route(received);
            }
            RawEnvelope filtered = envelopeFilter.apply(received);
            PayloadTransformer.Outcome outcome;
            try {
                outcome = transformer == null ? new PayloadTransformer.Outcome(filtered, null) : transformer.apply(filtered);
            } catch (SchemaUnavailableException e) {
                // 스키마를 지금 못 가져옴: 기록하지 않고 실패로 끝내 상대가 다시 보내게 한다(무손실, DSC-09.03)
                log.warn("소스 {} 스키마 조회 실패, 확인하지 않습니다: {}", d.id(), e.getMessage());
                return java.util.concurrent.CompletableFuture.failedFuture(e);
            }
            RawEnvelope envelope = outcome.envelope();
            CompletionStage<Void> write = writer.write(envelope);
            return write.thenRun(() -> {
                if (outcome.counter() != null) {
                    counters.payload.computeIfAbsent(outcome.counter(), k -> new AtomicLong()).incrementAndGet();
                }
                counters.received.incrementAndGet();
                counters.bytes.addAndGet(envelope.payload().length);
                receivedMeter.increment();
                receivedListener.onReceived(envelope);
            });
        };
        ConnectorContext ctx = new ConnectorContext(properties.instanceId(), clock,
                status -> statusListener.onStatus(current(d.id(), d), status),
                lease == null ? null : leases.cursorStore(lease));
        try {
            ConnectorSession session = connector.open(config, sink, ctx);
            running.put(d.id(), new Running(d, config, session, counters, lease));
            if (d.paused()) {
                session.pause();
            }
            session.start();
            log.info("소스 {} 실행 시작(커넥터 {}, client-id {}, {})", d.id(), key, config.clientId(), d.lifecycle());
        } catch (InvalidSettingsException e) {
            releaseQuietly(lease);
            log.warn("소스 {} 설정 오류: {}", d.id(), e.getMessage());
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.PROTOCOL,
                    "SOURCE_CONFIG_INVALID: " + e.field()));
        } catch (RuntimeException e) {
            releaseQuietly(lease);
            log.warn("소스 {} 세션을 열 수 없습니다: {}", d.id(), e.toString());
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.OTHER, e.getClass().getSimpleName()));
        }
    }

    private SourceDefinition current(long id, SourceDefinition fallback) {
        Running r = running.get(id);
        return r == null ? fallback : r.definition();
    }

    /** BR-DSC-01: {@code {base}-{env}-{n}}, 개발자는 {@code {base}-dev-{이름}-{n}} */
    String clientId(SourceDefinition d) {
        String base = d.clientIdBase() != null && d.clientIdBase().matches("[a-z0-9][a-z0-9-]*")
                ? d.clientIdBase() : properties.mqtt().clientIdBase();
        String developer = properties.developer();
        if (developer != null && !developer.isBlank()) {
            return ClientIds.developer(base, developer.toLowerCase(Locale.ROOT), properties.ordinal());
        }
        return ClientIds.of(base, properties.env(), properties.ordinal());
    }

    private void drain(Running r) {
        if (r.session() instanceof DrainableSession drainable) {
            drainable.drainAndClose(properties.drainTimeout());
        } else {
            r.session().close();
        }
        releaseQuietly(r.lease());
    }

    private void releaseQuietly(LeaseManager.Held lease) {
        if (lease == null) {
            return;
        }
        try {
            leases.release(lease);
        } catch (RuntimeException e) {
            log.debug("소스 {} 리스 반납 실패(만료되면 넘어간다): {}", lease.sourceId(), e.toString());
        }
    }

    /**
     * 리스 주기 작업(BR-DSC-26, {@code lease.renew-every}): 가진 리스를 연장하고, 잃었으면 즉시 수집을 멈추고 대기로 돌린다. 대기 중인 소스는
     * 리스를 다시 시도해 얻으면 연다(리더가 죽으면 ttl + 주기 안에 넘겨받음).
     */
    public synchronized void leaseTick() {
        if (!started) {
            return;
        }
        for (Running r : new ArrayList<>(running.values())) {
            if (r.lease() == null) {
                continue;
            }
            boolean kept;
            try {
                kept = leases.renew(r.lease());
            } catch (RuntimeException e) {
                log.warn("소스 {} 리스 연장 실패: {}", r.definition().id(), e.toString());
                kept = false;
            }
            if (!kept) {
                log.warn("소스 {}: 리스를 잃어 수집을 멈춥니다(fencing token {})", r.definition().id(), r.lease().fencingToken());
                running.remove(r.definition().id());
                r.session().close();
                standby.put(r.definition().id(), r.definition());
            }
        }
        for (SourceDefinition d : new ArrayList<>(standby.values())) {
            try {
                open(d, new SourceCounters());
            } catch (RuntimeException e) {
                log.warn("소스 {} 리스 시도 실패: {}", d.id(), e.toString());
            }
        }
    }

    /** 리스를 기다리는 소스(상태 보고·시험용) */
    public Collection<Long> standby() {
        return List.copyOf(standby.keySet());
    }

    /** 지금 실행 중인 소스(상태 보고용 사본) */
    public Collection<Running> running() {
        return List.copyOf(running.values());
    }

    @Override
    public void start() {
        started = true;
        long every = properties.lease().renewEvery().toMillis();
        leaseTimer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("connector-lease").factory());
        leaseTimer.scheduleWithFixedDelay(this::leaseTick, every, every, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** 종료: 모든 세션을 동시에 drain한다. 새 메시지는 확인하지 않아 다른 ingress·재접속 뒤에 다시 받는다 */
    @Override
    public void stop() {
        List<Running> all;
        synchronized (this) {
            started = false;
            if (leaseTimer != null) {
                leaseTimer.shutdownNow();
            }
            all = new ArrayList<>(running.values());
            running.clear();
            standby.clear();
        }
        List<Thread> threads = all.stream().map(r -> Thread.ofVirtual().start(() -> drain(r))).toList();
        for (Thread t : threads) {
            try {
                t.join(properties.drainTimeout().plusSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.info("소스 세션 {}개를 정리했습니다", all.size());
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
