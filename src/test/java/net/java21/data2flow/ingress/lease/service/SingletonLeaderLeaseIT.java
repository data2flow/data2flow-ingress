package net.java21.data2flow.ingress.lease.service;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.connector.LeaseLostException;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.PollCursorStore;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.http.HttpPollingConnector;
import net.java21.data2flow.ingress.connector.http.RecordsHttpServer;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.connector.webhook.ReplayGuard;
import net.java21.data2flow.ingress.lease.repository.LeaseRepository;
import net.java21.data2flow.ingress.lease.repository.WebhookRequestRepository;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.IngressFixtures;
import net.java21.data2flow.ingress.support.MutableClock;
import net.java21.data2flow.ingress.support.PostgresTestDb;
import net.java21.data2flow.ingress.webhook.service.JdbcReplayGuard;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.10 TC-DSC-292·295 BR-DSC-26 ADR-052: SINGLETON 커넥터 리더 리스가 실제 PostgreSQL 18(스키마 data2flow_ingress, 운영과 같은 Flyway)
 * 에서 동작한다. 리더 1대만 폴링하고, 리더가 멈추면 다른 인스턴스가 리스를 넘겨받아(fencing token 증가) 저장된 커서부터 이어 읽고, 리스를 잃은
 * 쪽의 커서 저장은 거부된다.
 */
class SingletonLeaderLeaseIT {

    static HikariDataSource ds;
    static LeaseRepository repo;
    static final AtomicLong IDS = new AtomicLong(900 + (System.nanoTime() & 0xFFFF));

    @BeforeAll
    static void db() {
        ds = PostgresTestDb.dataSource();
        repo = new LeaseRepository(new JdbcTemplate(ds));
    }

    @AfterAll
    static void close() {
        ds.close();
    }

    @Test
    @DisplayName("DSC-09.10 TC-DSC-295 리더 1대, 30초 만료 뒤 넘겨받으면 token+1, 잃은 쪽 커서 저장은 LeaseLostException")
    void leaseTakeoverAndFencing() {
        long source = IDS.incrementAndGet();
        MutableClock clock = new MutableClock(Instant.parse("2026-10-04T00:00:00Z"));
        JdbcLeaseManager a = new JdbcLeaseManager(repo, "ingress-a", Duration.ofSeconds(30), clock);
        JdbcLeaseManager b = new JdbcLeaseManager(repo, "ingress-b", Duration.ofSeconds(30), clock);

        LeaseManager.Held la = a.tryAcquire(1, source).orElseThrow();
        assertThat(b.tryAcquire(1, source)).as("다른 인스턴스가 리더면 대기").isEmpty();
        assertThat(a.tryAcquire(1, source)).as("같은 인스턴스는 같은 token으로 연장").contains(la);
        PollCursorStore storeA = a.cursorStore(la);
        storeA.save(source, PollCursor.at("10"));
        clock.advance(Duration.ofSeconds(20));
        assertThat(a.renew(la)).isTrue();

        clock.advance(Duration.ofSeconds(31));   // A가 갱신하지 못함(죽음·분리)
        LeaseManager.Held lb = b.tryAcquire(1, source).orElseThrow();
        assertThat(lb.fencingToken()).isEqualTo(la.fencingToken() + 1);
        assertThat(a.renew(la)).as("잃은 리스는 연장되지 않는다").isFalse();
        assertThatThrownBy(() -> storeA.save(source, PollCursor.at("11"))).isInstanceOf(LeaseLostException.class);

        PollCursorStore storeB = b.cursorStore(lb);
        assertThat(storeB.load(source)).contains(PollCursor.at("10"));
        storeB.save(source, new PollCursor("10", "page-2"));
        assertThat(storeB.load(source)).contains(new PollCursor("10", "page-2"));
        assertThat(a.tryAcquire(1, source)).isEmpty();

        b.release(lb);
        assertThat(a.tryAcquire(1, source).orElseThrow().fencingToken()).as("반납하면 바로 넘겨받는다").isEqualTo(lb.fencingToken() + 1);
    }

    @Test
    @DisplayName("DSC-07.04 BR-DSC-10 재생 방지 기록은 인스턴스 사이에 공유되고 10분 뒤 잊는다")
    void replayGuardIsShared() {
        long source = IDS.incrementAndGet();
        WebhookRequestRepository r = new WebhookRequestRepository(new JdbcTemplate(ds));
        ReplayGuard g1 = new JdbcReplayGuard(r);
        ReplayGuard g2 = new JdbcReplayGuard(r);
        Instant now = Instant.parse("2026-10-04T00:00:00Z");
        assertThat(g1.seen(1, source, "req-1", now)).isFalse();
        g1.remember(1, source, "req-1", now);
        assertThat(g2.seen(1, source, "req-1", now.plusSeconds(60))).isTrue();
        assertThat(g2.seen(1, source, "req-1", now.plus(ReplayGuard.WINDOW).plusSeconds(1))).isFalse();
        assertThat(((JdbcReplayGuard) g2).purge(now.plus(Duration.ofHours(1)))).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("DSC-09.10 TC-DSC-292 두 ingress 중 리더만 HTTP 폴링, 리더가 분리되면 다른 쪽이 리스·커서를 넘겨받아 유실 없이 이어 읽는다(60초 이내)")
    void supervisorsFailOver() throws Exception {
        long source = IDS.incrementAndGet();
        RecordsHttpServer server = new RecordsHttpServer(new InMemoryPollCursorStore(), source);
        List<RawEnvelope> writtenA = new CopyOnWriteArrayList<>();
        List<RawEnvelope> writtenB = new CopyOnWriteArrayList<>();
        FreezableLeases leasesA = new FreezableLeases(new JdbcLeaseManager(repo, "ingress-a", Duration.ofSeconds(2), Clock.systemUTC()));
        SourceSupervisor a = supervisor("ingress-a", leasesA, writtenA);
        SourceSupervisor b = supervisor("ingress-b", new JdbcLeaseManager(repo, "ingress-b", Duration.ofSeconds(2), Clock.systemUTC()), writtenB);
        try {
            SourceDefinition def = pollSource(source, server.url());
            a.apply(new RuntimeConfigSnapshot("1", List.of(def)));
            b.apply(new RuntimeConfigSnapshot("1", List.of(def)));
            assertThat(a.running()).hasSize(1);
            assertThat(b.running()).isEmpty();
            assertThat(b.standby()).containsExactly(source);

            server.publish(records("first", 50));
            await().atMost(Duration.ofSeconds(30)).until(() -> writtenA.size() >= 50);
            assertThat(writtenB).as("대기 인스턴스는 폴링하지 않는다").isEmpty();

            long started = System.nanoTime();
            leasesA.freeze();   // A는 자기가 리더라고 믿지만 DB 리스는 갱신되지 않는다(네트워크 분리)
            await().atMost(Duration.ofSeconds(60)).until(() -> b.running().size() == 1);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(60));
            server.publish(records("second", 50));
            await().atMost(Duration.ofSeconds(30)).until(() -> payloads(writtenB).containsAll(new HashSet<>(asText(records("second", 50)))));

            Set<String> all = payloads(writtenA);
            all.addAll(payloads(writtenB));
            assertThat(all).containsAll(asText(records("first", 50))).containsAll(asText(records("second", 50)));
            assertThat(b.running().iterator().next().lease().fencingToken()).isGreaterThan(leasesA.lastToken);
        } finally {
            a.stop();
            b.stop();
            server.close();
        }
    }

    private static SourceSupervisor supervisor(String instance, LeaseManager leases, List<RawEnvelope> written) {
        IngressProperties p = IngressFixtures.props("dev", null, List.of(), List.of(), Map.of());
        IngressProperties props = new IngressProperties(instance, 0, p.env(), p.developer(), p.coreUri(), p.resyncInterval(),
                p.reportInterval(), p.statsInterval(), Duration.ofSeconds(2), p.autoStart(), p.sourceFilter(), p.credentials(),
                p.mqtt(), p.stream(), p.connectionTest(), p.live(), p.platformBroker(), p.signing(), p.db(),
                new IngressProperties.Lease(Duration.ofSeconds(2), Duration.ofMillis(500)),
                new IngressProperties.Polling(Duration.ofMillis(100)), p.webhook(), p.payload());
        HttpPollingConnector http = new HttpPollingConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)),
                Clock.systemUTC());
        SourceSupervisor s = new SourceSupervisor(props, new ConnectorRegistry(List.of(http)), e -> {
            written.add(e);
            return CompletableFuture.completedFuture(null);
        }, (d, st) -> { }, e -> { }, new SimpleMeterRegistry(), Clock.systemUTC());
        s.useLeaseManager(leases);
        s.start();
        return s;
    }

    private static SourceDefinition pollSource(long id, String url) {
        ObjectNode c = JsonMapper.builder().build().createObjectNode().put("url", url).put("intervalSec", 1)
                .put("itemsPath", "/items").put("cursorParam", "since").put("nextCursorPath", "/next")
                .put("pageSizeParam", "limit").put("pageSize", 20);
        return new SourceDefinition(id, 1, SourceTypes.CONNECTOR, SourceDefinition.ACTIVE, HttpPollingConnector.KEY, c,
                Map.of(), null);
    }

    private static List<byte[]> records(String tag, int n) {
        return java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> ("{\"tag\":\"" + tag + "\",\"i\":" + i + "}").getBytes(StandardCharsets.UTF_8)).toList();
    }

    private static Set<String> payloads(List<RawEnvelope> list) {
        Set<String> s = new HashSet<>();
        list.forEach(e -> s.add(new String(e.payload(), StandardCharsets.UTF_8)));
        return s;
    }

    private static List<String> asText(List<byte[]> list) {
        return list.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
    }

    /** 얼리면 DB 리스를 갱신하지 않으면서 갱신했다고 답한다(분리된 리더) */
    static final class FreezableLeases implements LeaseManager {
        private final LeaseManager delegate;
        private volatile boolean frozen;
        volatile long lastToken;

        FreezableLeases(LeaseManager delegate) {
            this.delegate = delegate;
        }

        void freeze() {
            frozen = true;
        }

        @Override
        public Optional<Held> tryAcquire(long organizationId, long sourceId) {
            Optional<Held> h = frozen ? Optional.empty() : delegate.tryAcquire(organizationId, sourceId);
            h.ifPresent(x -> lastToken = x.fencingToken());
            return h;
        }

        @Override
        public boolean renew(Held lease) {
            return frozen || delegate.renew(lease);
        }

        @Override
        public void release(Held lease) {
            if (!frozen) {
                delegate.release(lease);
            }
        }

        @Override
        public PollCursorStore cursorStore(Held lease) {
            return delegate.cursorStore(lease);
        }
    }
}
