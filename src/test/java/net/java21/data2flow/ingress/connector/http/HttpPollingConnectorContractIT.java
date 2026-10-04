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
 * DSC-09.09 TC-DSC-246·290 BR-DSC-24: HTTP 폴링 커넥터가 실제 HTTP 서버(증분 커서 {@code since}, 페이지 크기 {@code limit})를 상대로
 * 계약 키트 전체(1,000건 무손실, 기록 전 위치 저장 금지, 기록 실패 시 같은 위치부터 다시 읽기, 일시정지/재개, 재시작 후 이어 읽기·중복 없음)를
 * 통과한다. 확인 수 = 저장된 폴링 위치.
 */
class HttpPollingConnectorContractIT extends AbstractConnectorContractTest {

    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final RecordsHttpServer server = new RecordsHttpServer(store, 36);

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Override
    protected SourceConnector connector() {
        return new HttpPollingConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("url", server.url());
        c.put("intervalSec", 1);
        c.put("itemsPath", "/items");
        c.put("cursorParam", "since");
        c.put("nextCursorPath", "/next");
        c.put("pageSizeParam", "limit");
        c.put("pageSize", 100);
        return new SourceConfig(1, 36, SourceTypes.CONNECTOR, HttpPollingConnector.KEY, c, null, null);
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
