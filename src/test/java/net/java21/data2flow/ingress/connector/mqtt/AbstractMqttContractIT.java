package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * MQTT 커넥터 계약 IT 공통 부분(DSC-09.02, BR-DSC-23). Testcontainers Mosquitto에 발행하고, 확인 수는 ingress와 브로커 사이 프록시가
 * 실제 PUBACK 패킷으로 센다. 하위 클래스는 전송 방식(tcp·ws·wss·mTLS)과 MQTT 버전만 정한다.
 */
abstract class AbstractMqttContractIT extends AbstractConnectorContractTest {

    static final JsonMapper JSON = JsonMapper.builder().build();
    static final String TOPIC_FILTER = "kit/+/up";

    private static MqttSourceConnector connector;
    private static MqttTestPublisher publisher;
    private static MqttAckCountingProxy proxy;

    /** 커넥터가 접속할 URL(프록시·nginx 경유) */
    protected abstract String url();

    /** 이 클래스가 시험하는 MQTT 버전 */
    protected abstract String version();

    /** 설정 추가(인증·TLS 등) */
    protected void customize(ObjectNode config) {
    }

    /** 비밀값(인증·TLS) */
    protected Map<String, Secret> secrets() {
        return Map.of();
    }

    /** 이 클래스 고유의 client-id(같은 브로커를 여러 IT가 함께 써도 겹치지 않게) */
    protected abstract String clientId();

    static GenericContainer<?> broker() {
        return MqttTestBroker.shared();
    }

    /** 브로커 앞 확인 계수 프록시(클래스마다 하나) */
    static synchronized MqttAckCountingProxy proxy(boolean webSocket) {
        if (proxy == null) {
            try {
                proxy = new MqttAckCountingProxy(broker().getHost(),
                        broker().getMappedPort(webSocket ? MqttTestBroker.WS_PORT : MqttTestBroker.MQTT_PORT), webSocket);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return proxy;
    }

    static synchronized MqttTestPublisher publisher() {
        if (publisher == null) {
            publisher = new MqttTestPublisher(broker().getHost(), broker().getMappedPort(MqttTestBroker.MQTT_PORT),
                    "kit/" + Long.toHexString(System.nanoTime()) + "/up");
        }
        return publisher;
    }

    @AfterAll
    static void tearDownShared() {
        if (connector != null) {
            connector.close();
            connector = null;
        }
        if (publisher != null) {
            publisher.close();
            publisher = null;
        }
        if (proxy != null) {
            proxy.close();
            proxy = null;
        }
    }

    @Override
    protected SourceConnector connector() {
        synchronized (AbstractMqttContractIT.class) {
            if (connector == null) {
                connector = new MqttSourceConnector(MqttConnectorOptions.defaults().withTestTimeout(Duration.ofSeconds(3)),
                        clock());
            }
            return connector;
        }
    }

    @Override
    protected SourceConfig sourceConfig() {
        ObjectNode config = JSON.createObjectNode();
        config.put("url", url());
        config.put("version", version());
        config.put("keepaliveSec", 30);
        config.put("cleanStart", false);
        config.put("sessionExpirySec", 600);
        config.putArray("topics").addObject().put("topic", TOPIC_FILTER).put("qos", 1);
        customize(config);
        return new SourceConfig(1, 3, SourceTypes.MQTT_SUBSCRIBE, MqttSourceConnector.KEY, config, secrets(), clientId());
    }

    @Override
    protected ContractPeer peer() {
        boolean ws = url().startsWith("ws");
        MqttAckCountingProxy p = proxy(ws);
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) {
                publisher().publishAll(payloads);
            }

            @Override
            public long acknowledgedCount() {
                return p.acknowledgedCount();
            }
        };
    }
}
