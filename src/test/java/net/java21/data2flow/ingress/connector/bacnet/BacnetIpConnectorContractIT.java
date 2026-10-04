package net.java21.data2flow.ingress.connector.bacnet;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.02·09.14 TC-DSC-326: BACnet/IP 커넥터(직접 구현, BACnet4J GPL 미사용)가 시험 BACnet 장치(실제 UDP)를 상대로 추세 기록 모드에서 계약
 * 키트 전체(1,000건 무손실, 기록 전 번호 저장 금지, 기록 실패 시 같은 번호부터, 재시작 후 이어 읽기·중복 없음)를 통과한다. Present_Value 모드는
 * 값 읽기와 응답 없음 → 재연결을 본다. 상대 장치가 시험 코드라는 한계는 ASHRAE Annex F 바이트 대조(BacnetCodecTest)로 보완한다.
 */
class BacnetIpConnectorContractIT extends AbstractConnectorContractTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final BacnetDeviceSimulator device;
    private final long run = ThreadLocalRandom.current().nextLong(1, 400_000);
    private long seq;

    BacnetIpConnectorContractIT() throws Exception {
        device = new BacnetDeviceSimulator(store, 48);
    }

    @AfterEach
    void stopDevice() {
        device.close();
    }

    @Override
    protected SourceConnector connector() {
        return new BacnetIpConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JSON.createObjectNode().put("host", "127.0.0.1").put("port", device.port())
                .put("deviceInstance", BacnetDeviceSimulator.DEVICE).put("mode", "trend-log").put("trendLogInstance", 1)
                .put("intervalSec", 1).put("batchSize", 50);
        return new SourceConfig(1, 48, SourceTypes.CONNECTOR, BacnetIpConnector.KEY, c, null, null);
    }

    /** 추세 기록 하나. 번호는 장치가 1부터 매기므로 같은 순서로 만든다 */
    @Override
    protected List<byte[]> payloads(int count) {
        List<byte[]> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long s = ++seq;
            String time = Instant.parse("2026-10-04T00:00:00Z").plusSeconds(s).toString();
            list.add(("{\"device\":1234,\"log\":1,\"seq\":" + s + ",\"time\":\"" + time + "\",\"value\":" + (run * 10_000 + s) + "}")
                    .getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Override
    protected ContractPeer peer() {
        return device;
    }

    @Override
    protected PollCursorStore cursorStore() {
        return store;
    }

    @Test
    @DisplayName("DSC-09.02 Present_Value 읽기 {device, values}, 장치가 응답하지 않으면 끊김 → 응답하면 다시 읽는다")
    void presentValueAndRecovery() throws Exception {
        device.analogInputs.put(3, 21.5f);
        ObjectNode c = JSON.createObjectNode().put("host", "127.0.0.1").put("port", device.port())
                .put("deviceInstance", BacnetDeviceSimulator.DEVICE).put("intervalSec", 1);
        c.putArray("points").addObject().put("name", "zoneTemp").put("objectType", "analog-input").put("instance", 3);
        RecordingRawSink sink = new RecordingRawSink();
        ConnectorSession s = connector().open(new SourceConfig(1, 49, SourceTypes.CONNECTOR, BacnetIpConnector.KEY, c, null, null),
                sink, new ConnectorContext("bacnet-it", Clock.systemUTC(), x -> { }, new InMemoryPollCursorStore()));
        try {
            s.start();
            await().atMost(Duration.ofSeconds(20)).until(() -> sink.writtenCount() >= 1);
            assertThat(new String(sink.written().get(0).payload(), StandardCharsets.UTF_8))
                    .isEqualTo("{\"device\":1234,\"values\":{\"zoneTemp\":21.5}}");
            device.mute = true;
            await().atMost(Duration.ofSeconds(30)).until(() -> s.status().state() != ConnectorState.CONNECTED);
            device.analogInputs.put(3, 23.0f);
            device.mute = false;
            await().atMost(Duration.ofSeconds(60)).until(() -> sink.written().stream()
                    .anyMatch(e -> new String(e.payload(), StandardCharsets.UTF_8).contains("23.0")));
        } finally {
            s.close();
        }
    }
}
