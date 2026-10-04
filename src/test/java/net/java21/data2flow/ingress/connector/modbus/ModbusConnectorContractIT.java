package net.java21.data2flow.ingress.connector.modbus;

import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.PollCursorStore;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import net.java21.data2flow.contracts.test.connector.RecordingRawSink;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-05.01·09.09 TC-DSC-251·131·132 BR-DSC-24: Modbus TCP 커넥터가 j2mod 슬레이브(실제 TCP)를 상대로 계약 키트 전체를 통과한다. 기록 모드
 * (기록 카운터 + 고리형 기록 영역)로 1,000건 무손실, 기록 전 위치 저장 금지, 기록 실패 시 같은 위치부터, 재시작 후 이어 읽기·중복 없음을 본다.
 * 측정점 모드는 AT-DSC-11.1(FLOAT32 40001 × 0.1 = 22.5)과 연결 끊김 뒤 자동으로 다시 읽기를 본다.
 */
class ModbusConnectorContractIT extends AbstractConnectorContractTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final ModbusTestSlave slave;
    private final int run = ThreadLocalRandom.current().nextInt(1, 65535);
    private int seq;

    ModbusConnectorContractIT() throws Exception {
        slave = new ModbusTestSlave(store, 46);
    }

    @AfterEach
    void stopSlave() {
        slave.close();
    }

    @Override
    protected SourceConnector connector() {
        return new ModbusTcpConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JSON.createObjectNode().put("host", "127.0.0.1").put("port", slave.port).put("unitId", 1)
                .put("mode", "log").put("intervalSec", 1);
        c.putObject("log").put("counterAddress", 0).put("recordAddress", ModbusTestSlave.RECORD_BASE)
                .put("recordLength", ModbusTestSlave.RECORD).put("ringSize", ModbusTestSlave.RING);
        return new SourceConfig(1, 46, SourceTypes.MODBUS_TCP, ModbusTcpConnector.KEY, c, null, null);
    }

    /** 기록 하나 = 레지스터 4개. 이 실행에서만 쓰는 값이라 이전 시나리오의 기록과 섞이지 않는다 */
    @Override
    protected List<byte[]> payloads(int count) {
        List<byte[]> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int s = seq++;
            list.add(("{\"unitId\":1,\"registers\":[" + run + "," + ((s >>> 16) & 0xFFFF) + "," + (s & 0xFFFF) + ",48879]}")
                    .getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Override
    protected ContractPeer peer() {
        return slave;
    }

    @Override
    protected PollCursorStore cursorStore() {
        return store;
    }

    private SourceConfig snapshotConfig() {
        ObjectNode c = JSON.createObjectNode().put("host", "127.0.0.1").put("port", slave.port).put("unitId", 1)
                .put("intervalSec", 1);
        var points = c.putArray("points");
        points.addObject().put("name", "temperature").put("register", "40011").put("type", "FLOAT32").put("scale", 0.1);
        points.addObject().put("name", "outdoor").put("register", "30001").put("type", "INT16");
        return new SourceConfig(1, 47, SourceTypes.MODBUS_TCP, ModbusTcpConnector.KEY, c, null, null);
    }

    @Test
    @DisplayName("DSC-05.01 AT-DSC-11.1 FLOAT32 레지스터 읽기 값 225, 배율 0.1 → 22.5, INT16 -40, 끊겼다 복구되면 자동으로 다시 읽는다")
    void snapshotScalingAndReconnect() throws Exception {
        RecordingRawSink sink = new RecordingRawSink();
        ConnectorSession s = connector().open(snapshotConfig(), sink,
                new ConnectorContext("modbus-it", Clock.systemUTC(), x -> { }, new InMemoryPollCursorStore()));
        try {
            s.start();
            await().atMost(Duration.ofSeconds(20)).until(() -> sink.writtenCount() >= 1);
            String first = new String(sink.written().get(0).payload(), StandardCharsets.UTF_8);
            assertThat(first).isEqualTo("{\"unitId\":1,\"values\":{\"temperature\":22.5,\"outdoor\":-40}}");

            slave.stop();
            await().atMost(Duration.ofSeconds(20)).until(() -> s.status().state() != ConnectorState.CONNECTED);
            slave.setFloat(10, 230.0f);
            slave.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> sink.written().stream()
                    .anyMatch(e -> new String(e.payload(), StandardCharsets.UTF_8).contains("\"temperature\":23")));
            assertThat(s.status().state()).isEqualTo(ConnectorState.CONNECTED);
        } finally {
            s.close();
        }
    }
}
