package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.ingress.connector.mqtt.MqttConnectorOptions;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import org.junit.jupiter.api.AfterAll;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * MQTT 프리셋 커넥터 계약 IT 공통: 시험 Mosquitto에 그 서비스 모양의 토픽으로 발행하고, 확인 수는 커넥터와 브로커 사이 프록시가 실제
 * PUBACK 패킷으로 센다(AbstractMqttContractIT와 같은 방식).
 */
abstract class AbstractMqttPresetContractIT extends AbstractConnectorContractTest {

    static final JsonMapper JSON = JsonMapper.builder().build();
    private static MqttSourceConnector mqtt;
    private static MqttTestPublisher publisher;
    private static MqttAckCountingProxy proxy;

    /** 시험할 프리셋 */
    protected abstract MqttPresetConnector preset(MqttSourceConnector mqtt);

    /** 프리셋 설정. {@code proxyUrl}은 브로커 앞 프록시 주소 */
    protected abstract ObjectNode presetConfig(String proxyUrl);

    /** 발행 토픽(프리셋이 구독하는 모양) */
    protected abstract String publishTopic();

    protected Map<String, Secret> secrets() {
        return Map.of();
    }

    protected abstract long sourceId();

    /** 프록시 앞 URL(기본 tcp) */
    protected String proxyUrl() {
        return "tcp://localhost:" + proxy().port();
    }

    static synchronized MqttAckCountingProxy proxy() {
        if (proxy == null) {
            try {
                proxy = new MqttAckCountingProxy(MqttTestBroker.shared().getHost(),
                        MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT), false);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return proxy;
    }

    synchronized MqttTestPublisher publisher() {
        if (publisher == null) {
            publisher = new MqttTestPublisher(MqttTestBroker.shared().getHost(),
                    MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT), publishTopic());
        }
        return publisher;
    }

    @AfterAll
    static void tearDownShared() {
        if (mqtt != null) {
            mqtt.close();
            mqtt = null;
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
        synchronized (AbstractMqttPresetContractIT.class) {
            if (mqtt == null) {
                mqtt = new MqttSourceConnector(MqttConnectorOptions.defaults().withTestTimeout(Duration.ofSeconds(3)), clock());
            }
            return preset(mqtt);
        }
    }

    @Override
    protected SourceConfig sourceConfig() {
        return new SourceConfig(1, sourceId(), SourceTypes.CONNECTOR, connector().descriptor().key(),
                presetConfig(proxyUrl()), secrets(), "data2flow-ingress-it-" + connector().descriptor().key() + "-0");
    }

    @Override
    protected ContractPeer peer() {
        MqttAckCountingProxy p = proxy();
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
