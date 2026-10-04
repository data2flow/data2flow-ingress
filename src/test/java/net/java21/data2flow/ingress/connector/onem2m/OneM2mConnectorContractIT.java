package net.java21.data2flow.ingress.connector.onem2m;

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
 * DSC-05.01·09.09 TC-DSC-131·132·325: oneM2M 커넥터가 Mobius HTTP 바인딩 흉내 CSE를 상대로 계약 키트 전체(1,000건 무손실, 기록 전 위치 저장
 * 금지, 기록 실패 시 같은 위치부터, 재시작 후 이어 읽기·중복 없음)를 통과한다. 확인 수 = 저장된 stateTag.
 */
class OneM2mConnectorContractIT extends AbstractConnectorContractTest {

    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final MobiusMockCse cse;

    OneM2mConnectorContractIT() throws Exception {
        cse = new MobiusMockCse(store, 50);
    }

    @AfterEach
    void stopCse() {
        cse.close();
    }

    @Override
    protected SourceConnector connector() {
        return new OneM2mConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("cseUrl", cse.cseUrl());
        c.putArray("containers").add("kit-ae/kit-cnt");
        c.put("intervalSec", 1);
        c.put("batchSize", 100);
        return new SourceConfig(1, 50, SourceTypes.ONEM2M, OneM2mConnector.KEY, c, null, null);
    }

    @Override
    protected ContractPeer peer() {
        return cse;
    }

    @Override
    protected PollCursorStore cursorStore() {
        return store;
    }
}
