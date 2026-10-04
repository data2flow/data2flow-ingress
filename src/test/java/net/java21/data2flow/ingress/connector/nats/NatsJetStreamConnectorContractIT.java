package net.java21.data2flow.ingress.connector.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DSC-09.02·09.03 TC-DSC-245: NATS JetStream 커넥터가 nats:2(-js) 컨테이너를 상대로 계약 키트 공통 시나리오를 통과한다.
 * 확인 수 = 보낸 수 − (아직 전달 안 됨 + 전달됐지만 ack 대기). 서버의 소비자 정보로 센다.
 */
class NatsJetStreamConnectorContractIT extends AbstractConnectorContractTest {

    static final GenericContainer<?> NATS = new GenericContainer<>("nats:2.10-alpine").withCommand("-js")
            .withExposedPorts(4222).waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
    static final String STREAM = "KIT";
    static final String DURABLE = "data2flow-ingress-it";
    static final AtomicLong PUBLISHED = new AtomicLong();
    static Connection peer;

    @BeforeAll
    static void startNats() throws Exception {
        NATS.start();
        peer = Nats.connect("nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
        JetStreamManagement jsm = peer.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder().name(STREAM).subjects("kit.>").storageType(StorageType.File).build());
    }

    @AfterAll
    static void stopNats() throws Exception {
        peer.close();
        NATS.stop();
    }

    @Override
    protected SourceConnector connector() {
        return new NatsJetStreamConnector(PollingOptions.defaults().withIdleInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("url", "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
        c.put("stream", STREAM);
        c.put("subject", "kit.>");
        c.put("durable", DURABLE);
        c.put("batchSize", 100);
        return new SourceConfig(1, 34, SourceTypes.CONNECTOR, NatsJetStreamConnector.KEY, c, null, "data2flow-ingress-it-nats-0");
    }

    @Override
    protected ContractPeer peer() {
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) throws Exception {
                JetStream js = peer.jetStream();
                for (byte[] p : payloads) {
                    js.publish("kit.sensor", p);
                }
                PUBLISHED.addAndGet(payloads.size());
            }

            @Override
            public long acknowledgedCount() {
                try {
                    ConsumerInfo ci = peer.jetStreamManagement().getConsumerInfo(STREAM, DURABLE);
                    return PUBLISHED.get() - ci.getNumPending() - ci.getNumAckPending();
                } catch (Exception e) {
                    return 0;   // 소비자가 아직 없음
                }
            }
        };
    }
}
