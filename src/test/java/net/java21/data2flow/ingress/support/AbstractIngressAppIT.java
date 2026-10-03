package net.java21.data2flow.ingress.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 앱 전체를 띄우는 IT 공통 설정: Testcontainers RabbitMQ(AMQP + Stream)·Mosquitto, core-api 대역({@link CoreStub}).
 * 공용 인프라(s3·s4·iot-data)에는 붙지 않는다(CLAUDE.md §5).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"management.server.port=0", "data2flow.ingress.instance-id=ingress-it-0",
                "data2flow.ingress.env=dev", "data2flow.ingress.developer=it",
                "data2flow.ingress.report-interval=2s", "data2flow.ingress.stats-interval=2s"})
public abstract class AbstractIngressAppIT {

    protected static final CoreStub CORE = new CoreStub();

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        RabbitTestBroker.properties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("data2flow.ingress.core-uri", CORE::uri);
        MqttTestBroker.shared();
    }

    protected static String brokerUrl() {
        return "tcp://" + MqttTestBroker.shared().getHost() + ":" + MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT);
    }

    protected static MqttTestPublisher publisher(String topic) {
        return new MqttTestPublisher(MqttTestBroker.shared().getHost(),
                MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT), topic);
    }
}
