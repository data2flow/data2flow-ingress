package net.java21.data2flow.ingress.source.event;

import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.event.ConnectorCatalogReported;
import net.java21.data2flow.contracts.message.event.SourceStatsReported;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 소스 연결 상태·수신 통계·커넥터 카탈로그를 core-api에 이벤트로 보고한다(DSC-02.01·02.03·09.02).
 *
 * <ul>
 *   <li>EVT-DSC-02 {@code source.runtime.reported}: 상태가 바뀌면 즉시, 그리고 30초마다 모든 소스.</li>
 *   <li>EVT-DSC-03 {@code source.stats.1m}: 1분마다 소스별 {@code received}·{@code bytes}·{@code reconnects}(분 단위로 자른 시각).</li>
 *   <li>EVT-DSC-09 {@code connector.catalog.reported}: 시작할 때 커넥터 목록.</li>
 * </ul>
 * 발행은 별도 스레드에서 해서 수신 경로를 막지 않는다.
 */
public class SourceStatusReporter implements SourceSupervisor.StatusListener, SourceSupervisor.ReceivedListener, AutoCloseable {

    private final SourceEventPublisher events;
    private final IngressProperties properties;
    private final ConnectorRegistry registry;
    private final Clock clock;
    private final long catalogOrganizationId;
    private final ExecutorService publisher =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("source-status").factory());
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("source-report").factory());
    private final Map<Long, long[]> lastStats = new ConcurrentHashMap<>();
    private final Map<Long, Integer> lastReconnects = new ConcurrentHashMap<>();
    private final Map<String, Long> lastExtra = new ConcurrentHashMap<>();
    private SourceSupervisor supervisor;

    /**
     * @param catalogOrganizationId 카탈로그 이벤트 봉투의 organizationId(카탈로그는 조직과 무관하지만 봉투가 요구한다. v1 단일 조직, ADR-004)
     */
    public SourceStatusReporter(SourceEventPublisher events, IngressProperties properties, ConnectorRegistry registry,
                                Clock clock, long catalogOrganizationId) {
        this.events = events;
        this.properties = properties;
        this.registry = registry;
        this.clock = clock;
        this.catalogOrganizationId = catalogOrganizationId;
    }

    public void attach(SourceSupervisor supervisor) {
        this.supervisor = supervisor;
    }

    /** 주기 보고 시작 */
    public void start() {
        publisher.execute(this::reportCatalog);
        long report = properties.reportInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::reportAll, report, report, TimeUnit.MILLISECONDS);
        long stats = properties.statsInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::reportStats, stats, stats, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStatus(SourceDefinition source, ConnectorStatus status) {
        publisher.execute(() -> events.publish(DomainEvent.of(EventType.SOURCE_RUNTIME_REPORTED, source.organizationId(),
                status.toReport(source.id(), properties.instanceId()), null, clock)));
    }

    @Override
    public void onReceived(RawEnvelope envelope) {
        // 통계는 SourceSupervisor.SourceCounters에서 모은다
    }

    void reportAll() {
        if (supervisor == null) {
            return;
        }
        for (SourceSupervisor.Running r : supervisor.running()) {
            onStatus(r.definition(), r.session().status());
        }
    }

    void reportStats() {
        if (supervisor == null) {
            return;
        }
        Instant minute = clock.instant().truncatedTo(ChronoUnit.MINUTES);
        for (SourceSupervisor.Running r : supervisor.running()) {
            long id = r.definition().id();
            long received = r.counters().received();
            long bytes = r.counters().bytes();
            int reconnects = r.session().status().reconnects24h();
            long[] last = lastStats.getOrDefault(id, new long[]{0, 0});
            int lastRe = lastReconnects.getOrDefault(id, reconnects);
            lastStats.put(id, new long[]{received, bytes});
            lastReconnects.put(id, reconnects);
            Map<String, Long> counters = new LinkedHashMap<>();
            counters.put("received", Math.max(0, received - last[0]));
            counters.put("bytes", Math.max(0, bytes - last[1]));
            counters.put("reconnects", (long) Math.max(0, reconnects - lastRe));
            if (r.session() instanceof net.java21.data2flow.ingress.connector.common.SessionCounters extra) {
                // Webhook 서명 실패·시각 오차·재생 거부 등(TC-DSC-186): 세션 누적값의 1분 차이
                extra.counters().forEach((name, total) -> {
                    Long prev = lastExtra.put(id + ":" + name, total);
                    counters.put(name, Math.max(0, total - (prev == null ? 0 : prev)));
                });
            }
            SourceStatsReported payload = new SourceStatsReported(id, minute, SourceStatsReported.Producer.INGRESS, counters);
            publisher.execute(() -> events.publish(DomainEvent.of(EventType.SOURCE_STATS_1M,
                    r.definition().organizationId(), payload, null, clock)));
        }
    }

    void reportCatalog() {
        events.publish(DomainEvent.of(EventType.CONNECTOR_CATALOG_REPORTED, catalogOrganizationId,
                new ConnectorCatalogReported(properties.instanceId(), registry.catalog()), null, clock));
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        publisher.shutdown();
    }
}
