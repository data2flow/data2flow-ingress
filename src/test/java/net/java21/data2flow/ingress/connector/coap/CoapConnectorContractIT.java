package net.java21.data2flow.ingress.connector.coap;

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
 * DSC-09.02 TC-DSC-249: CoAP observe 커넥터가 Californium CoAP 서버(실제 UDP)를 상대로 계약 키트 공통 시나리오(관찰 등록, 알림 1,000건을
 * 빠짐없이 RawEnvelope로, 일시정지 동안 받은 알림은 재개 뒤 넘김, 상태 보고·닫기)를 통과한다. 확인 방식이 NONE이라 확인 시점 시나리오는
 * 건너뛴다(화면 "유실 가능"). DTLS는 아직 시험하지 않는다.
 */
class CoapConnectorContractIT extends AbstractConnectorContractTest {

    static CoapTestServer server;

    @BeforeAll
    static void startServer() {
        server = new CoapTestServer();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @Override
    protected SourceConnector connector() {
        return new CoapConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.putArray("resources").add(server.uri());
        return new SourceConfig(1, 39, SourceTypes.CONNECTOR, CoapConnector.KEY, c, null, null);
    }

    @Override
    protected ContractPeer peer() {
        return server;
    }
}
