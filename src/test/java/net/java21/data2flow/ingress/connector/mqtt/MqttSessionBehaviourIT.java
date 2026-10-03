package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.RecordingRawSink;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

/**
 * 연결 단위 동작(Testcontainers Mosquitto): DSC-07.01 일시정지는 세션을 남긴 채 끊고, 재개하면 그동안 쌓인 메시지부터 받는다.
 */
class MqttSessionBehaviourIT {

    static final JsonMapper JSON = JsonMapper.builder().build();
    private final MqttSourceConnector connector = new MqttSourceConnector();
    private ConnectorSession session;

    @AfterEach
    void close() {
        if (session != null) {
            session.close();
        }
        connector.close();
    }

    SourceConfig config(String version, String topic, String clientId) {
        ObjectNode c = JSON.createObjectNode();
        c.put("url", "tcp://" + MqttTestBroker.shared().getHost() + ":" + MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT));
        c.put("version", version);
        c.putArray("topics").addObject().put("topic", topic).put("qos", 1);
        return new SourceConfig(1, 9, SourceTypes.MQTT_SUBSCRIBE, "mqtt", c, Map.of(), clientId);
    }

    @ParameterizedTest
    @ValueSource(strings = {MqttSourceSettings.V5, MqttSourceSettings.V311})
    @DisplayName("DSC-07.01 TC-DSC-163 일시정지(세션 유지) 중 발행된 메시지를 재개 뒤 모두 받는다")
    void pauseKeepsSession(String version) {
        String topic = "pause/" + UUID.randomUUID() + "/up";
        RecordingRawSink sink = new RecordingRawSink();
        session = connector.open(config(version, topic, "data2flow-ingress-it-pause-" + version.replace(".", "")), sink,
                new ConnectorContext("it-0", Clock.systemUTC(), s -> { }));
        session.start();
        await().atMost(Duration.ofSeconds(20)).until(() -> session.status().state() == ConnectorState.CONNECTED);
        session.pause();
        try (MqttTestPublisher pub = new MqttTestPublisher(MqttTestBroker.shared().getHost(),
                MqttTestBroker.shared().getMappedPort(MqttTestBroker.MQTT_PORT), topic)) {
            for (int i = 0; i < 5; i++) {
                pub.publish(("{\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8));
            }
        }
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> sink.attempts() == 0);
        session.resume();
        await().atMost(Duration.ofSeconds(30)).until(() -> sink.written().stream().map(RawEnvelope::payload)
                .map(p -> new String(p, StandardCharsets.UTF_8)).distinct().count() == 5);
    }
}
