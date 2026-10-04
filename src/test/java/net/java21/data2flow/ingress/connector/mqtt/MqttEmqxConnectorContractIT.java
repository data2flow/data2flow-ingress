package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;

/**
 * DSC-09.02·09.04 TC-DSC-238(MQTT 변형): MQTT 5.0 + 공유 구독({@code $share/kit/…})으로 EMQX 5(오픈소스판) 컨테이너를 상대로 계약 키트
 * 공통 시나리오를 통과한다. Mosquitto와 다른 브로커에서도 수동 PUBACK·영속 세션 재전송이 같게 동작하는지 본다. 확인 수는 프록시가 센다.
 */
class MqttEmqxConnectorContractIT extends AbstractConnectorContractTest {

    static final GenericContainer<?> EMQX = new GenericContainer<>("emqx/emqx:5.8.6").withExposedPorts(1883)
            .withEnv("EMQX_MQTT__MAX_MQUEUE_LEN", "200000")
            .waitingFor(Wait.forListeningPorts(1883).withStartupTimeout(Duration.ofMinutes(3)));
    static MqttAckCountingProxy proxy;
    static MqttTestPublisher publisher;
    static MqttSourceConnector connector;

    @BeforeAll
    static void start() throws Exception {
        EMQX.start();
        proxy = new MqttAckCountingProxy(EMQX.getHost(), EMQX.getMappedPort(1883), false);
        publisher = new MqttTestPublisher(EMQX.getHost(), EMQX.getMappedPort(1883), "kit/emqx/up");
        connector = new MqttSourceConnector(MqttConnectorOptions.defaults().withTestTimeout(Duration.ofSeconds(3)),
                java.time.Clock.systemUTC());
    }

    @AfterAll
    static void stop() {
        connector.close();
        publisher.close();
        proxy.close();
        EMQX.stop();
    }

    @Override
    protected SourceConnector connector() {
        return connector;
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("url", "tcp://localhost:" + proxy.port());
        c.put("version", MqttSourceSettings.V5);
        c.put("cleanStart", false);
        c.put("sessionExpirySec", 600);
        c.put("sharedGroup", "kit");
        c.putArray("topics").addObject().put("topic", "kit/+/up").put("qos", 1);
        return new SourceConfig(1, 44, SourceTypes.MQTT_SUBSCRIBE, MqttSourceConnector.KEY, c, null,
                "data2flow-ingress-it-emqx-0");
    }

    @Override
    protected ContractPeer peer() {
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) {
                publisher.publishAll(payloads);
            }

            @Override
            public long acknowledgedCount() {
                return proxy.acknowledgedCount();
            }
        };
    }
}
