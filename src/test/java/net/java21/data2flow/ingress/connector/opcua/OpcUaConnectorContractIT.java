package net.java21.data2flow.ingress.connector.opcua;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-05.01·09.02 TC-DSC-250·132 AT-DSC-11.2: OPC UA 구독 커넥터가 Eclipse Milo 데모 서버 컨테이너를 상대로 계약 키트 공통 시나리오(구독,
 * 값 변화 300건을 빠짐없이 RawEnvelope로, 일시정지 동안 받은 알림은 재개 뒤 넘김, 상태 보고·닫기)를 통과한다. 보안 정책은 None으로 시험한다
 * (Basic256Sha256은 설정만 지원, 시험 미실시). 확인 방식 NONE이라 확인 시점 시나리오는 건너뛴다.
 */
class OpcUaConnectorContractIT extends AbstractConnectorContractTest {

    static final OpcUaDemoServer SERVER = new OpcUaDemoServer();

    @BeforeAll
    static void start() throws Exception {
        SERVER.start();
    }

    @AfterAll
    static void stop() throws Exception {
        SERVER.close();
    }

    @Override
    protected SourceConnector connector() {
        return new OpcUaConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        return config("value");
    }

    private SourceConfig config(String payload) {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("endpointUrl", SERVER.endpointUrl());
        c.putArray("nodes").addObject().put("nodeId", OpcUaDemoServer.NODE).put("name", "kit/string");
        c.put("samplingIntervalMs", 0);
        c.put("publishingIntervalMs", 10);
        c.put("queueSize", 1000);
        c.put("payload", payload);
        return new SourceConfig(1, 45, SourceTypes.OPCUA, OpcUaConnector.KEY, c, null, null);
    }

    /**
     * 데모 서버의 표본 주기(약 0.1~0.2초)마다 값 하나만 보이므로 1,000건이면 3분이 넘는다. 표본 구독이라 건수가 늘어도 검증하는 성질이
     * 같아 300건으로 줄인다(TC-DSC-250의 1,000건과 다른 점은 test-plan에 적었다).
     */
    @Override
    protected int messageCount() {
        return 300;
    }

    @Override
    protected Duration timeout() {
        return Duration.ofSeconds(120);
    }

    @Override
    protected ContractPeer peer() {
        return SERVER;
    }

    @Test
    @DisplayName("DSC-05.01 AT-DSC-11.2 json 모양: nodeId·value·sourceTime·status, 값이 바뀔 때만 보낸다")
    void jsonPayloadOnChange() throws Exception {
        var session = connector().open(config("json"), sink(), new net.java21.data2flow.contracts.connector.ConnectorContext(
                INSTANCE_ID, Clock.systemUTC(), s -> { }));
        try {
            session.start();
            await().atMost(Duration.ofSeconds(30)).until(() -> session.status().state()
                    == net.java21.data2flow.contracts.connector.ConnectorState.CONNECTED);
            SERVER.publish(List.of("22.5".getBytes(StandardCharsets.UTF_8)));
            await().atMost(Duration.ofSeconds(30)).until(() -> sink().written().stream()
                    .anyMatch(e -> new String(e.payload(), StandardCharsets.UTF_8).contains("\"value\":\"22.5\"")));
            RawEnvelope e = sink().written().stream()
                    .filter(x -> new String(x.payload(), StandardCharsets.UTF_8).contains("22.5")).findFirst().orElseThrow();
            JsonNode n = JsonMapper.builder().build().readTree(e.payload());
            assertThat(n.path("nodeId").asString()).isEqualTo(OpcUaDemoServer.NODE);
            assertThat(n.path("status").asString()).isEqualTo("GOOD");
            assertThat(n.has("sourceTime") || n.has("serverTime")).isTrue();
            assertThat(e.topic()).isEqualTo("kit/string");
            int before = sink().writtenCount();
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> sink().writtenCount() == before);
        } finally {
            session.close();
        }
    }
}
