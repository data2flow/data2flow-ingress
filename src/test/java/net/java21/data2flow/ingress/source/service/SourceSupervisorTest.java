package net.java21.data2flow.ingress.source.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import net.java21.data2flow.ingress.support.FakeConnector;
import net.java21.data2flow.ingress.support.IngressFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class SourceSupervisorTest {

    static final JsonMapper JSON = JsonMapper.builder().build();
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-04T00:00:00Z"), ZoneOffset.UTC);

    FakeConnector connector;
    List<ConnectorStatus> statuses;
    List<RawEnvelope> written;
    List<RawEnvelope> received;
    SourceSupervisor supervisor;

    static IngressProperties props(String env, String developer, List<Long> orgs, List<String> denied) {
        return IngressFixtures.props(env, developer, orgs, denied);
    }

    @BeforeEach
    void setUp() {
        setUp(props("prod", null, List.of(), List.of("iot-data.java21.net")));
    }

    void setUp(IngressProperties properties) {
        connector = new FakeConnector();
        statuses = new CopyOnWriteArrayList<>();
        written = new CopyOnWriteArrayList<>();
        received = new CopyOnWriteArrayList<>();
        supervisor = new SourceSupervisor(properties, new ConnectorRegistry(List.of(connector)), e -> {
            written.add(e);
            return CompletableFuture.completedFuture(null);
        }, (d, s) -> statuses.add(s), received::add, new SimpleMeterRegistry(), CLOCK);
        supervisor.start();
    }

    static SourceDefinition source(long id, String lifecycle, String url, String topic) {
        return IngressFixtures.source(id, lifecycle, url, topic);
    }

    static RuntimeConfigSnapshot snapshot(String v, SourceDefinition... sources) {
        return new RuntimeConfigSnapshot(v, List.of(sources));
    }

    @Test
    @DisplayName("BR-DSC-05 DSC-01.01 활성 소스를 열고 시작한다. client-id는 {base}-{env}-{n}(BR-DSC-01, 파드 이름 끝 번호)")
    void opensActiveSource() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a/#")));
        assertThat(connector.sessions).hasSize(1);
        FakeConnector.Session s = connector.sessions.getFirst();
        assertThat(s.started).isTrue();
        assertThat(s.paused).isFalse();
        assertThat(s.config.clientId()).isEqualTo("data2flow-ingress-prod-1");
        assertThat(s.config.connectorKey()).isEqualTo("mqtt");
        assertThat(s.ctx.instanceId()).isEqualTo("data2flow-ingress-1");
        assertThat(statuses).extracting(ConnectorStatus::state).contains(ConnectorState.CONNECTED);
        assertThat(supervisor.running()).hasSize(1);
    }

    @Test
    @DisplayName("BR-DSC-01 로컬 개발자는 {base}-dev-{이름}-{n}, 소스가 준 base를 쓴다")
    void developerClientId() {
        setUp(props("dev", "nhn", List.of(), List.of()));
        SourceDefinition d = source(3, "ACTIVE", "tcp://broker", "a");
        d = new SourceDefinition(d.id(), 1, d.type(), d.lifecycle(), null, d.config(), d.secrets(), "data2flow-chirpstack-s3");
        supervisor.apply(snapshot("1", d));
        assertThat(connector.sessions.getFirst().config.clientId()).isEqualTo("data2flow-chirpstack-s3-dev-nhn-1");
    }

    @Test
    @DisplayName("BR-DSC-05 같은 설정이 다시 오면 아무것도 하지 않는다(멱등)")
    void sameConfigIsNoOp() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        supervisor.apply(snapshot("2", source(3, "ACTIVE", "tcp://broker", "a")));
        assertThat(connector.sessions).hasSize(1);
        assertThat(connector.sessions.getFirst().closed).isFalse();
    }

    @Test
    @DisplayName("DSC-07.01 lifecycle만 바뀌면 일시정지(세션 유지)·재개로 처리한다")
    void lifecycleChangePausesAndResumes() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        supervisor.apply(snapshot("2", source(3, "PAUSED", "tcp://broker", "a")));
        FakeConnector.Session s = connector.sessions.getFirst();
        assertThat(s.paused).isTrue();
        assertThat(s.closed).isFalse();
        supervisor.apply(snapshot("3", source(3, "ACTIVE", "tcp://broker", "a")));
        assertThat(s.paused).isFalse();
        assertThat(connector.sessions).hasSize(1);
    }

    @Test
    @DisplayName("DSC-07.01 PAUSED로 처음 받은 소스는 일시정지한 채 연다")
    void pausedSourceOpensPaused() {
        supervisor.apply(snapshot("1", source(3, "PAUSED", "tcp://broker", "a")));
        assertThat(connector.sessions.getFirst().paused).isTrue();
    }

    @Test
    @DisplayName("BR-DSC-05 연결 설정이 바뀐 소스만 다시 맺는다(다른 소스 영향 없음)")
    void changedSourceReconnectsOnlyThatSource() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a"), source(4, "ACTIVE", "tcp://broker", "b")));
        supervisor.apply(snapshot("2", source(3, "ACTIVE", "tcp://broker", "a2"), source(4, "ACTIVE", "tcp://broker", "b")));
        assertThat(connector.sessions).hasSize(3);
        assertThat(connector.sessions.get(0).drained).as("소스 3 이전 세션").isTrue();
        assertThat(connector.sessions.get(1).closed).as("소스 4").isFalse();
        assertThat(connector.sessions.get(2).config.config().path("topics").get(0).path("topic").asString()).isEqualTo("a2");
    }

    @Test
    @DisplayName("BR-DSC-05 비밀값이 바뀌면 다시 맺는다(DSC-07.02 교체)")
    void secretChangeReconnects() {
        SourceDefinition a = source(3, "ACTIVE", "tcp://broker", "a");
        supervisor.apply(snapshot("1", a));
        SourceDefinition b = new SourceDefinition(3, 1, a.type(), a.lifecycle(), null, a.config(),
                Map.of("PASSWORD", Secret.of("new")), null);
        supervisor.apply(snapshot("2", b));
        assertThat(connector.sessions).hasSize(2);
        assertThat(connector.sessions.getFirst().drained).isTrue();
    }

    @Test
    @DisplayName("BR-DSC-05 목록에서 빠지면 drain 후 닫는다")
    void removedSourceIsDrained() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        supervisor.apply(snapshot("2"));
        assertThat(connector.sessions.getFirst().drained).isTrue();
        assertThat(supervisor.running()).isEmpty();
    }

    @Test
    @DisplayName("deployment.md §9 staging은 공용 브로커를 구독하지 않는다(denied-hosts), 조직 필터")
    void sourceFilter() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "wss://iot-data.java21.net:443/mqtt", "application/#")));
        assertThat(connector.sessions).isEmpty();
        setUp(props("stg", null, List.of(7L), List.of()));
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        assertThat(connector.sessions).as("조직 1은 staging 조직(7)이 아니다").isEmpty();
    }

    @Test
    @DisplayName("ingress가 실행하지 않는 유형(SIMULATION·WEBHOOK)과 없는 커넥터")
    void unsupportedTypes() {
        SourceDefinition sim = new SourceDefinition(5, 1, SourceTypes.SIMULATION, "ACTIVE", null, JSON.createObjectNode(), Map.of(), null);
        SourceDefinition kafka = new SourceDefinition(6, 1, SourceTypes.CONNECTOR, "ACTIVE", "kafka", JSON.createObjectNode(), Map.of(), null);
        supervisor.apply(snapshot("1", sim, kafka));
        assertThat(connector.sessions).isEmpty();
        assertThat(statuses).anyMatch(s -> s.state() == ConnectorState.ERROR && s.errorMessage().contains("CONNECTOR_UNAVAILABLE"));
    }

    @Test
    @DisplayName("설정 오류로 세션을 열 수 없으면 ERROR를 보고하고 다른 소스는 계속한다")
    void invalidConfigReportsError() {
        connector.failOpen = true;
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        assertThat(statuses).anyMatch(s -> s.state() == ConnectorState.ERROR && s.errorMessage().contains("SOURCE_CONFIG_INVALID"));
        assertThat(supervisor.running()).isEmpty();
    }

    @Test
    @DisplayName("DSC-02.03 기록이 끝난 메시지만 소스 통계와 실시간 보기로 넘긴다")
    void sinkCountsAfterWrite() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a")));
        FakeConnector.Session s = connector.sessions.getFirst();
        RawEnvelope e = s.ctx.envelope(s.config, "a", "{\"t\":1}".getBytes(StandardCharsets.UTF_8));
        s.sink.write(e).toCompletableFuture().join();
        assertThat(written).containsExactly(e);
        assertThat(received).containsExactly(e);
        SourceSupervisor.Running r = new ArrayList<>(supervisor.running()).getFirst();
        assertThat(r.counters().received()).isEqualTo(1);
        assertThat(r.counters().bytes()).isEqualTo(7);
    }

    static SourceDefinition withConfig(SourceDefinition d, String extraJson) {
        tools.jackson.databind.node.ObjectNode c = (tools.jackson.databind.node.ObjectNode) d.config().deepCopy();
        JSON.readTree(extraJson).properties().forEach(e -> c.set(e.getKey(), e.getValue()));
        return new SourceDefinition(d.id(), d.organizationId(), d.type(), d.lifecycle(), d.connectorKey(), c, d.secrets(),
                d.clientIdBase());
    }

    void usePayload() {
        supervisor.usePayloadTransformers(new net.java21.data2flow.ingress.payload.service.PayloadTransformerFactory(
                new net.java21.data2flow.ingress.payload.schema.PayloadSchemaClient(org.springframework.web.client.RestClient
                        .builder().requestFactory(net.java21.data2flow.ingress.payload.schema.PayloadSchemaClient
                                .requestFactory(java.time.Duration.ofMillis(300))), JSON, "http://127.0.0.1:1"),
                new net.java21.data2flow.ingress.payload.schema.AvroRegistryClient(java.time.Duration.ofMillis(300), JSON),
                1024 * 1024, JSON));
    }

    @Test
    @DisplayName("DSC-09.07·09.08 TC-DSC-280·285 기록 직전에 변환·템플릿을 적용하고, 기록 뒤에 소스 통계(converted·unmatchedTopic)를 올린다")
    void payloadTransformAppliedBeforeWrite() {
        usePayload();
        supervisor.apply(snapshot("1", withConfig(source(3, "ACTIVE", "tcp://broker", "site/#"),
                "{\"payload\":{\"format\":\"cbor\"},\"topicTemplate\":\"site/{site}/{deviceId}\"}")));
        FakeConnector.Session s = connector.sessions.getFirst();
        byte[] cbor = net.java21.data2flow.ingress.payload.PayloadTestData.cbor();
        s.sink.write(s.ctx.envelope(s.config, "site/a/em-1", cbor)).toCompletableFuture().join();
        s.sink.write(s.ctx.envelope(s.config, "other", cbor)).toCompletableFuture().join();
        assertThat(written.get(0).payloadFormat()).isEqualTo("CBOR");
        assertThat(written.get(0).originalPayload()).isEqualTo(cbor);
        assertThat(written.get(0).topicAttributes()).containsEntry("externalId", "em-1");
        assertThat(written.get(1).ingressStatus()).isEqualTo("UNMATCHED_TOPIC");
        assertThat(received).hasSize(2);
        SourceSupervisor.Running r = new ArrayList<>(supervisor.running()).getFirst();
        assertThat(r.counters().payloadCounters()).containsEntry("converted", 1L).containsEntry("unmatchedTopic", 1L);
    }

    @Test
    @DisplayName("DSC-09.03 스키마를 가져올 수 없으면 기록하지 않고 실패로 끝낸다(확인하지 않음 → 상대 재전송)")
    void schemaUnavailableFailsWrite() {
        usePayload();
        supervisor.apply(snapshot("1", withConfig(source(3, "ACTIVE", "tcp://broker", "a"),
                "{\"payload\":{\"format\":\"protobuf\",\"schemaRef\":\"9\"}}")));
        FakeConnector.Session s = connector.sessions.getFirst();
        var result = s.sink.write(s.ctx.envelope(s.config, "a", new byte[]{1})).toCompletableFuture();
        assertThat(result).isCompletedExceptionally();
        assertThat(written).isEmpty();
        assertThat(new ArrayList<>(supervisor.running()).getFirst().counters().received()).isZero();
    }

    @Test
    @DisplayName("DSC-09.07 payload 설정 오류(모르는 형식·잘못된 템플릿)는 세션을 열지 않고 SOURCE_CONFIG_INVALID(필드 이름)")
    void invalidPayloadSettings() {
        usePayload();
        supervisor.apply(snapshot("1", withConfig(source(3, "ACTIVE", "tcp://broker", "a"),
                "{\"payload\":{\"format\":\"xml\"}}")));
        assertThat(connector.sessions).isEmpty();
        assertThat(statuses).anyMatch(st -> st.state() == ConnectorState.ERROR
                && st.errorMessage().contains("SOURCE_CONFIG_INVALID: payload.format"));
    }

    @Test
    @DisplayName("TC-ING-021 종료하면 모든 세션을 drain하고 그 뒤 설정은 반영하지 않는다")
    void stopDrainsAll() {
        supervisor.apply(snapshot("1", source(3, "ACTIVE", "tcp://broker", "a"), source(4, "ACTIVE", "tcp://broker", "b")));
        supervisor.stop();
        assertThat(connector.sessions).allMatch(s -> s.drained);
        assertThat(supervisor.isRunning()).isFalse();
        supervisor.apply(snapshot("2", source(5, "ACTIVE", "tcp://broker", "c")));
        assertThat(connector.sessions).hasSize(2);
        assertThat(supervisor.getPhase()).isGreaterThan(net.java21.data2flow.ingress.stream.service.RawStreamWriter.PHASE);
    }
}
