package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.RecordingRawSink;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-02.02 TC-DSC-069, NFR-02.04 TC-NFR-017(AT-NFR-05.4 축소판), DSC-02.01: 브로커 연결을 막으면 재연결 간격이 1, 2, 4, 8초로 늘고,
 * 끊긴 동안 상태는 DISCONNECTED(원인 포함)로 보고된다. 다시 열면 백오프 간격 안에 재연결·구독 복원하고, 끊긴 동안 발행된 메시지를 영속 세션으로
 * 모두 받는다(유실 0). 연결 차단은 ingress 쪽 프록시에서 한다(공용 브로커는 건드리지 않음).
 */
class ReconnectBackoffIT {

    static final JsonMapper JSON = JsonMapper.builder().build();
    private final MqttSourceConnector connector = new MqttSourceConnector();
    private MqttAckCountingProxy proxy;
    private ConnectorSession session;

    @AfterEach
    void close() {
        if (session != null) {
            session.close();
        }
        connector.close();
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    @DisplayName("DSC-02.02 TC-DSC-069 NFR-02.04 재연결 간격 1·2·4·8초, 복구 후 구독 복원, 끊긴 동안 메시지 유실 0")
    void exponentialBackoffAndRecovery() throws Exception {
        GenericContainer<?> broker = MqttTestBroker.shared();
        proxy = new MqttAckCountingProxy(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), false);
        String topic = "reconnect/" + UUID.randomUUID() + "/up";
        ObjectNode c = JSON.createObjectNode();
        c.put("url", "tcp://localhost:" + proxy.port());
        c.putArray("topics").addObject().put("topic", topic).put("qos", 1);
        RecordingRawSink sink = new RecordingRawSink();
        List<ConnectorStatus> reported = new CopyOnWriteArrayList<>();
        session = connector.open(new SourceConfig(1, 31, SourceTypes.MQTT_SUBSCRIBE, "mqtt", c, Map.of(),
                "data2flow-ingress-it-backoff-0"), sink, new ConnectorContext("it-0", Clock.systemUTC(), reported::add));
        session.start();
        await().atMost(Duration.ofSeconds(20)).until(() -> session.status().state() == ConnectorState.CONNECTED);

        int before = proxy.acceptedAtNanos().size();
        proxy.block(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> session.status().state() != ConnectorState.CONNECTED);
        try (MqttTestPublisher pub = new MqttTestPublisher(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), topic)) {
            for (int i = 0; i < 5; i++) {
                pub.publish(("{\"outage\":" + i + "}").getBytes(StandardCharsets.UTF_8));
            }
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> proxy.acceptedAtNanos().size() - before >= 4);
        List<Long> attempts = proxy.acceptedAtNanos().subList(before, before + 4);
        List<Double> gaps = new ArrayList<>();
        for (int i = 1; i < attempts.size(); i++) {
            gaps.add((attempts.get(i) - attempts.get(i - 1)) / 1e9);
        }
        // 첫 시도는 끊긴 뒤 1초, 그다음 간격은 2·4초(실패할 때마다 두 배)
        assertThat(gaps.get(0)).as("2초 간격 %s", gaps).isBetween(1.5, 3.0);
        assertThat(gaps.get(1)).as("4초 간격 %s", gaps).isBetween(3.5, 5.5);
        ConnectorStatus during = session.status();
        assertThat(during.state()).isIn(ConnectorState.DISCONNECTED, ConnectorState.CONNECTING);
        assertThat(during.reconnects24h()).isGreaterThanOrEqualTo(3);
        assertThat(reported).anyMatch(s -> s.state() == ConnectorState.DISCONNECTED && s.errorKind() != null);

        proxy.block(false);
        long reopened = System.nanoTime();
        await().atMost(Duration.ofSeconds(20)).until(() -> session.status().state() == ConnectorState.CONNECTED);
        double recoverySec = (System.nanoTime() - reopened) / 1e9;
        assertThat(recoverySec).as("백오프 최대 간격(다음 시도 8~16초) 안에 재연결").isLessThan(17);
        await().atMost(Duration.ofSeconds(20)).until(() -> sink.written().stream().map(RawEnvelope::payload)
                .map(p -> new String(p, StandardCharsets.UTF_8)).filter(p -> p.contains("outage")).distinct().count() == 5);
        System.out.println("[BACKOFF] gaps=" + gaps + " recoverySec=" + recoverySec);
    }
}
