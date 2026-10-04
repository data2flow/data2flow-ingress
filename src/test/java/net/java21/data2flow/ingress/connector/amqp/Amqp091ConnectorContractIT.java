package net.java21.data2flow.ingress.connector.amqp;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.support.QueueTestBroker;
import org.junit.jupiter.api.BeforeAll;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DSC-09.02·09.03 TC-DSC-242 AT-DSC-20.1: AMQP 0-9-1 커넥터가 RabbitMQ 4 컨테이너(클래식 큐)를 상대로 계약 키트 공통 시나리오(1,000건 무손실·
 * 기록 전 ack 금지·기록 실패 시 nack 재전달·일시정지/재개·상태 보고)를 통과한다. 확인 수 = 보낸 수 − 큐에 남은 수(ready + unacked).
 */
class Amqp091ConnectorContractIT extends AbstractConnectorContractTest {

    static final String QUEUE = "kit-amqp091";
    static final AtomicLong PUBLISHED = new AtomicLong();

    @BeforeAll
    static void declare() throws Exception {
        QueueTestBroker.declareQueue(QUEUE, "classic");
    }

    @Override
    protected SourceConnector connector() {
        return new Amqp091Connector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(50)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("url", "amqp://" + QueueTestBroker.host() + ":" + QueueTestBroker.amqpPort());
        c.put("queue", QUEUE);
        c.put("batchSize", 50);
        return new SourceConfig(1, 31, SourceTypes.CONNECTOR, Amqp091Connector.KEY, c, null, "data2flow-ingress-it-amqp-0");
    }

    @Override
    protected ContractPeer peer() {
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) throws Exception {
                QueueTestBroker.publish(QUEUE, payloads);
                PUBLISHED.addAndGet(payloads.size());
            }

            @Override
            public long acknowledgedCount() {
                return PUBLISHED.get() - QueueTestBroker.remaining(QUEUE);
            }
        };
    }
}
