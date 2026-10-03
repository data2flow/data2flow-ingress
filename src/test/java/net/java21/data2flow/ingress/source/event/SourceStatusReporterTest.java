package net.java21.data2flow.ingress.source.event;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.message.event.ConnectorCatalogReported;
import net.java21.data2flow.contracts.message.event.SourceRuntimeReported;
import net.java21.data2flow.contracts.message.event.SourceStatsReported;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.IngressFixtures;
import net.java21.data2flow.ingress.support.FakeConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SourceStatusReporterTest {

    static final MessageCodec CODEC = MessageCodec.create();
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-04T01:02:03Z"), ZoneOffset.UTC);

    final List<Message> sent = new CopyOnWriteArrayList<>();
    final List<String> keys = new CopyOnWriteArrayList<>();
    volatile boolean failing;
    final RabbitTemplate rabbit = new RabbitTemplate() {
        @Override
        public void send(String exchange, String routingKey, Message message) {
            if (failing) {
                throw new AmqpException("down");
            }
            assertThat(exchange).isEqualTo(MessagingNames.EXCHANGE_EVENTS);
            keys.add(routingKey);
            sent.add(message);
        }
    };
    final SourceEventPublisher events = new SourceEventPublisher(rabbit, CODEC, new SimpleMeterRegistry());
    SourceStatusReporter reporter;
    MqttSourceConnector mqtt;

    @AfterEach
    void close() {
        if (reporter != null) {
            reporter.close();
        }
        if (mqtt != null) {
            mqtt.close();
        }
    }

    List<DomainEvent<?>> events() {
        List<DomainEvent<?>> list = new java.util.ArrayList<>();
        sent.forEach(m -> list.add(CODEC.readEvent(m.getBody())));
        return list;
    }

    @Test
    @DisplayName("DSC-09.02 EVT-DSC-09 시작할 때 커넥터 카탈로그(키·버전·스키마·확인 방식·확장 방식)를 보고한다")
    void reportsCatalog() {
        mqtt = new MqttSourceConnector();
        reporter = new SourceStatusReporter(events, IngressFixtures.props("prod", null, List.of(), List.of()),
                new ConnectorRegistry(List.of(mqtt)), CLOCK, 1);
        reporter.reportCatalog();
        DomainEvent<?> e = events().getFirst();
        assertThat(keys).containsExactly("connector.catalog.reported");
        ConnectorCatalogReported p = (ConnectorCatalogReported) e.payload();
        assertThat(p.instanceId()).isEqualTo("data2flow-ingress-1");
        assertThat(p.connectors()).singleElement().satisfies(c -> {
            assertThat(c.key()).isEqualTo("mqtt");
            assertThat(c.version()).isEqualTo("1.0.0");
            assertThat(c.schema().path("type").asString()).isEqualTo("object");
            assertThat(c.supportsSend()).as("발행 경로 없음(CLAUDE.md §5)").isFalse();
        });
        MessageSchemas.assertValid(e);
        assertThat(sent.getFirst().getMessageProperties().getHeaders()).containsKeys("messageId", "v", "schema", "organizationId");
    }

    @Test
    @DisplayName("DSC-02.01 DSC-02.03 EVT-DSC-02 상태(30초)·EVT-DSC-03 분 통계(증가분, 분 단위 시각)를 보고하고, 발행 실패는 수집을 막지 않는다")
    void reportsRuntimeAndStats() throws Exception {
        FakeConnector fake = new FakeConnector();
        var props = IngressFixtures.props("prod", null, List.of(), List.of());
        reporter = new SourceStatusReporter(events, props, new ConnectorRegistry(List.of(fake)), CLOCK, 1);
        SourceSupervisor supervisor = new SourceSupervisor(props, new ConnectorRegistry(List.of(fake)),
                e -> CompletableFuture.completedFuture(null), reporter, reporter, new SimpleMeterRegistry(), CLOCK);
        reporter.attach(supervisor);
        supervisor.start();
        supervisor.apply(new RuntimeConfigSnapshot("1", List.of(IngressFixtures.source(3, "ACTIVE", "tcp://b", "a"))));
        FakeConnector.Session s = fake.sessions.getFirst();
        s.sink.write(s.ctx.envelope(s.config, "a", "hello".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();

        reporter.reportAll();
        reporter.reportStats();
        reporter.reportStats();
        await().until(() -> keys.stream().filter("source.stats.1m"::equals).count() == 2
                && keys.contains("source.runtime.reported"));
        SourceRuntimeReported runtime = events().stream().map(DomainEvent::payload)
                .filter(SourceRuntimeReported.class::isInstance).map(SourceRuntimeReported.class::cast).findFirst().orElseThrow();
        assertThat(runtime.sourceId()).isEqualTo(3);
        assertThat(runtime.state()).isEqualTo(ConnectorState.CONNECTED);
        List<SourceStatsReported> stats = events().stream().map(DomainEvent::payload)
                .filter(SourceStatsReported.class::isInstance).map(SourceStatsReported.class::cast).toList();
        assertThat(stats.get(0).counters()).containsEntry("received", 1L).containsEntry("bytes", 5L).containsEntry("reconnects", 0L);
        assertThat(stats.get(0).minute()).isEqualTo(Instant.parse("2026-10-04T01:02:00Z"));
        assertThat(stats.get(1).counters()).as("두 번째는 증가분만").containsEntry("received", 0L);
        events().forEach(MessageSchemas::assertValid);

        failing = true;
        reporter.onStatus(IngressFixtures.source(3, "ACTIVE", "tcp://b", "a"), ConnectorStatus.of(ConnectorState.CONNECTING));
        reporter.onReceived(null);
        supervisor.stop();
    }

    @Test
    @DisplayName("감시 대상이 없으면 주기 보고는 아무것도 하지 않는다")
    void noSupervisor() {
        reporter = new SourceStatusReporter(events, IngressFixtures.props("prod", null, List.of(), List.of()),
                new ConnectorRegistry(List.of()), CLOCK, 1);
        reporter.reportAll();
        reporter.reportStats();
        assertThat(sent).isEmpty();
    }
}
