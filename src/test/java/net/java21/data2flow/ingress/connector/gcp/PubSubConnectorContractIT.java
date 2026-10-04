package net.java21.data2flow.ingress.connector.gcp;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;

/**
 * DSC-09.02·09.03 TC-DSC-248: Google Pub/Sub 커넥터가 Pub/Sub REST 흉내 서버(pull·acknowledge·modifyAckDeadline, ack 기한 재전송)를
 * 상대로 계약 키트 공통 시나리오를 통과한다. 실제 GCP 계정이 없어 흉내 서버로 시험한다(ADR-040). 확인 수 = acknowledge 수.
 */
class PubSubConnectorContractIT extends AbstractConnectorContractTest {

    static PubSubMockServer server;

    @BeforeAll
    static void startServer() throws Exception {
        server = new PubSubMockServer();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @Override
    protected SourceConnector connector() {
        return new PubSubConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(50)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("endpoint", server.endpoint());
        c.put("project", "kit-project");
        c.put("subscription", "kit-sub");
        c.put("batchSize", 100);
        return new SourceConfig(1, 35, SourceTypes.CONNECTOR, PubSubConnector.KEY, c, null, null);
    }

    @Override
    protected ContractPeer peer() {
        return server;
    }
}
