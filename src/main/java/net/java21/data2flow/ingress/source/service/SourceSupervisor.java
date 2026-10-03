package net.java21.data2flow.ingress.source.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.ClientIds;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
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

    /** 실행 중인 소스 하나 */
    public record Running(SourceDefinition definition, SourceConfig config, ConnectorSession session,
                          SourceCounters counters) {
    }

    /** 소스별 누적 통계(EVT-DSC-03 원천) */
    public static final class SourceCounters {
        final AtomicLong received = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();

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
        for (SourceDefinition d : wanted.values()) {
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
                running.put(d.id(), new Running(d, r.config(), r.session(), r.counters()));
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

    private static String host(SourceDefinition d) {
        String url = d.config().path("url").asString(null);
        if (url == null) {
            return null;
        }
        try {
            String h = URI.create(url.trim()).getHost();
            return h == null ? null : h.toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private void open(SourceDefinition d, SourceCounters counters) {
        String key = ConnectorRegistry.connectorKeyFor(d.type(), d.connectorKey());
        SourceConnector connector = registry.find(key).orElse(null);
        if (connector == null) {
            log.warn("소스 {}: 커넥터 {}가 이 ingress에 없습니다(CONNECTOR_UNAVAILABLE)", d.id(), key);
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.OTHER, "CONNECTOR_UNAVAILABLE: " + key));
            return;
        }
        SourceConfig config = new SourceConfig(d.organizationId(), d.id(), d.type(), key, d.config(), d.secrets(),
                clientId(d));
        Counter receivedMeter = Counter.builder("data2flow.ingress.messages.received")
                .tag("sourceId", Long.toString(d.id())).description("스트림 기록까지 끝난 수신 메시지 수(DSC-02.03)")
                .register(meters);
        RawSink sink = received -> {
            RawEnvelope envelope = envelopeFilter.apply(received);
            CompletionStage<Void> write = writer.write(envelope);
            return write.thenRun(() -> {
                counters.received.incrementAndGet();
                counters.bytes.addAndGet(envelope.payload().length);
                receivedMeter.increment();
                receivedListener.onReceived(envelope);
            });
        };
        ConnectorContext ctx = new ConnectorContext(properties.instanceId(), clock,
                status -> statusListener.onStatus(current(d.id(), d), status));
        try {
            ConnectorSession session = connector.open(config, sink, ctx);
            running.put(d.id(), new Running(d, config, session, counters));
            if (d.paused()) {
                session.pause();
            }
            session.start();
            log.info("소스 {} 실행 시작(커넥터 {}, client-id {}, {})", d.id(), key, config.clientId(), d.lifecycle());
        } catch (InvalidSettingsException e) {
            log.warn("소스 {} 설정 오류: {}", d.id(), e.getMessage());
            statusListener.onStatus(d, ConnectorStatus.error(ConnectionErrorKind.PROTOCOL,
                    "SOURCE_CONFIG_INVALID: " + e.field()));
        } catch (RuntimeException e) {
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
    }

    /** 지금 실행 중인 소스(상태 보고용 사본) */
    public Collection<Running> running() {
        return List.copyOf(running.values());
    }

    @Override
    public void start() {
        started = true;
    }

    /** 종료: 모든 세션을 동시에 drain한다. 새 메시지는 확인하지 않아 다른 ingress·재접속 뒤에 다시 받는다 */
    @Override
    public void stop() {
        List<Running> all;
        synchronized (this) {
            started = false;
            all = new ArrayList<>(running.values());
            running.clear();
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
