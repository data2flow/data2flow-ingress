package net.java21.data2flow.ingress.support;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;

import java.util.List;
import java.util.UUID;

/**
 * 테스트 브로커(Testcontainers Mosquitto)에만 발행하는 테스트용 발행자. 센서·ChirpStack 역할을 흉내 낸다.
 * <b>운영 코드에는 발행 경로가 없고</b>, 이 클래스는 test 범위에만 있다(CLAUDE.md §5).
 */
public final class MqttTestPublisher implements AutoCloseable {

    private final Mqtt5BlockingClient client;
    private final String topic;

    public MqttTestPublisher(String host, int port, String topic) {
        if (host.contains("java21.net")) {
            throw new IllegalArgumentException("공용 브로커에는 발행하지 않습니다(CLAUDE.md §5)");
        }
        this.topic = topic;
        this.client = MqttClient.builder().useMqttVersion5().identifier("test-publisher-" + UUID.randomUUID())
                .serverHost(host).serverPort(port).buildBlocking();
        this.client.connect();
    }

    public void publish(byte[] payload) {
        client.publishWith().topic(topic).qos(MqttQos.AT_LEAST_ONCE).payload(payload).send();
    }

    public void publish(String topicOverride, byte[] payload) {
        client.publishWith().topic(topicOverride).qos(MqttQos.AT_LEAST_ONCE).payload(payload).send();
    }

    public void publishAll(List<byte[]> payloads) {
        payloads.forEach(this::publish);
    }

    @Override
    public void close() {
        client.disconnect();
    }
}
