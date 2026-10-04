package net.java21.data2flow.ingress.connector.http;

import net.java21.data2flow.contracts.connector.PollCursorStore;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterEach;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;

/**
 * DSC-09.09 TC-DSC-324 BR-DSC-24: SSE 커넥터가 실제 SSE 서버({@code Last-Event-ID} 재개)를 상대로
 * 계약 키트 전체(1,000건 무손실, 기록 전 위치 저장 금지, 기록 실패 시 같은 위치부터 다시 읽기, 일시정지/재개, 재시작 후 이어 읽기·중복 없음)를
 * 통과한다. 확인 수 = 저장된 마지막 이벤트 id.
 */
class SseConnectorContractIT extends AbstractConnectorContractTest {

    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final SseTestServer server = new SseTestServer(store, 37);

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Override
    protected SourceConnector connector() {
        return new SseConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("url", server.url());
        c.put("batchSize", 100);
        return new SourceConfig(1, 37, SourceTypes.CONNECTOR, SseConnector.KEY, c, null, null);
    }

    @Override
    protected ContractPeer peer() {
        return server;
    }

    @Override
    protected PollCursorStore cursorStore() {
        return store;
    }
}

