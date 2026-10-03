package net.java21.data2flow.ingress.source.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.stream.service.RawStreamWriter;
import net.java21.data2flow.ingress.support.FakeConnector;
import net.java21.data2flow.ingress.support.IngressFixtures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RuntimeConfigSyncTest {

    final IngressProperties props = IngressFixtures.props("prod", null, List.of(), List.of());
    final FakeConnector connector = new FakeConnector();
    final SourceSupervisor supervisor = new SourceSupervisor(props, new ConnectorRegistry(List.of(connector)),
            e -> CompletableFuture.completedFuture(null), (d, s) -> { }, e -> { }, new SimpleMeterRegistry(), Clock.systemUTC());
    final AtomicBoolean ready = new AtomicBoolean();
    final RawStreamWriter writer = new RawStreamWriter(props.stream(), Duration.ofSeconds(1), SuperStreamSpec.RAW,
            MessageCodec.create(), MessageTracing.noop(), new SimpleMeterRegistry()) {
        @Override
        public boolean isReady() {
            return ready.get();
        }
    };
    final AtomicInteger calls = new AtomicInteger();
    final AtomicInteger failuresLeft = new AtomicInteger(1);
    volatile String lastSince;
    final CoreSourceClient core = new CoreSourceClient(RestClient.builder(), JsonMapper.builder().build(), props) {
        @Override
        public Optional<RuntimeConfigSnapshot> fetch(String sinceVersion) {
            calls.incrementAndGet();
            lastSince = sinceVersion;
            if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("core down");
            }
            if ("2".equals(sinceVersion)) {
                return Optional.empty();
            }
            return Optional.of(new RuntimeConfigSnapshot("2",
                    List.of(IngressFixtures.source(3, "ACTIVE", "tcp://b", "a"))));
        }
    };
    final RuntimeConfigSync sync = new RuntimeConfigSync(core, supervisor, writer, props);

    @AfterEach
    void close() {
        sync.close();
        supervisor.stop();
    }

    @Test
    @DisplayName("BR-DSC-05 스트림이 준비된 뒤 설정을 읽고(core 장애면 백오프로 재시도), 변경 알림을 받으면 sinceVersion으로 다시 읽는다")
    void loadsAfterStreamReadyAndRetries() {
        supervisor.start();
        sync.requestRefresh();   // 아직 읽은 적 없으면 무시
        sync.start();
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2)).until(() -> calls.get() == 0);
        ready.set(true);
        await().atMost(Duration.ofSeconds(10)).until(sync::loaded);
        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
        assertThat(supervisor.running()).hasSize(1);

        int before = calls.get();
        sync.requestRefresh();
        sync.requestRefresh();   // 1초 안의 여러 변경은 한 번에
        await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() == before + 1);
        assertThat(lastSince).isEqualTo("2");
        assertThat(supervisor.running()).hasSize(1);
    }
}
