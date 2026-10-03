package net.java21.data2flow.ingress.connector.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.test.connector.RecordingRawSink;
import net.java21.data2flow.ingress.support.MqttTestBroker;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.10·09.04 BR-DSC-25: 확장 방식.
 * <ul>
 *   <li>TC-DSC-294 SCALABLE(MQTT 5 공유 구독): 인스턴스 2개가 1만 건을 중복 없이 나눠 받는다(합 10,000, 교집합 0).</li>
 *   <li>TC-DSC-296(ingress 쪽) DUAL_ACTIVE(MQTT 3.1.1): 두 인스턴스가 같은 메시지를 모두 받고, dedupKey가 같아 pipeline이 하나만 남긴다.</li>
 * </ul>
 */
class ConnectorScalingIT {

    static final JsonMapper JSON = JsonMapper.builder().build();
    private final MqttSourceConnector connector = new MqttSourceConnector();
    private final List<ConnectorSession> sessions = new ArrayList<>();

    @AfterEach
    void close() {
        sessions.forEach(ConnectorSession::close);
        connector.close();
    }

    SourceConfig config(String version, String topic, String shared, String clientId) {
        GenericContainer<?> b = MqttTestBroker.shared();
        ObjectNode c = JSON.createObjectNode();
        c.put("url", "tcp://" + b.getHost() + ":" + b.getMappedPort(MqttTestBroker.MQTT_PORT));
        c.put("version", version);
        c.put("cleanStart", true);
        if (shared != null) {
            c.put("sharedGroup", shared);
        }
        c.putArray("topics").addObject().put("topic", topic).put("qos", 1);
        return new SourceConfig(1, 41, SourceTypes.MQTT_SUBSCRIBE, "mqtt", c, Map.of(), clientId);
    }

    RecordingRawSink start(SourceConfig cfg, String instance) {
        RecordingRawSink sink = new RecordingRawSink();
        ConnectorSession s = connector.open(cfg, sink, new ConnectorContext(instance, Clock.systemUTC(), x -> { }));
        sessions.add(s);
        s.start();
        await().atMost(Duration.ofSeconds(20)).until(() -> s.status().state() == ConnectorState.CONNECTED);
        return sink;
    }

    static void publish(String topicPrefix, int count) {
        GenericContainer<?> b = MqttTestBroker.shared();
        Mqtt5AsyncClient pub = MqttClient.builder().useMqttVersion5().identifier("scale-pub-" + UUID.randomUUID())
                .serverHost(b.getHost()).serverPort(b.getMappedPort(MqttTestBroker.MQTT_PORT)).buildAsync();
        pub.connect().join();
        List<CompletableFuture<?>> all = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            all.add(pub.publishWith().topic(topicPrefix + (i % 10)).qos(MqttQos.AT_LEAST_ONCE)
                    .payload(("{\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8)).send());
            if (all.size() >= 500) {
                CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
                all.clear();
            }
        }
        CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
        pub.disconnect().join();
    }

    static Set<String> payloads(RecordingRawSink sink) {
        Set<String> set = new HashSet<>();
        sink.written().forEach(e -> set.add(new String(e.payload(), StandardCharsets.UTF_8)));
        return set;
    }

    @Test
    @DisplayName("DSC-09.10 TC-DSC-294 SCALABLE(MQTT 5 공유 구독): 2개 인스턴스가 1만 건을 중복 없이 나눠 받음(합 10,000, 교집합 0)")
    void sharedSubscriptionSplitsWithoutDuplicates() {
        String base = "scale/" + UUID.randomUUID() + "/";
        SourceConfig a = config(MqttSourceSettings.V5, base + "+", "ingress", "data2flow-ingress-it-scale-0");
        assertThat(connector.scaling(a)).isEqualTo(ScalingMode.SCALABLE);
        RecordingRawSink sinkA = start(a, "it-0");
        RecordingRawSink sinkB = start(config(MqttSourceSettings.V5, base + "+", "ingress", "data2flow-ingress-it-scale-1"), "it-1");
        publish(base, 10_000);
        await().atMost(Duration.ofSeconds(120)).until(() -> sinkA.writtenCount() + sinkB.writtenCount() >= 10_000);
        Set<String> pa = payloads(sinkA);
        Set<String> pb = payloads(sinkB);
        Set<String> both = new HashSet<>(pa);
        both.retainAll(pb);
        Set<String> union = new HashSet<>(pa);
        union.addAll(pb);
        assertThat(union).hasSize(10_000);
        assertThat(both).as("교집합").isEmpty();
        assertThat(pa).isNotEmpty();
        assertThat(pb).isNotEmpty();
        System.out.println("[SCALING] a=" + pa.size() + " b=" + pb.size());
    }

    @Test
    @DisplayName("DSC-09.10 TC-DSC-296 DUAL_ACTIVE(MQTT 3.1.1): 두 인스턴스가 모두 받고 같은 메시지의 dedupKey가 같다(ADR-015)")
    void dualActiveReceivesEverythingWithSameDedupKeys() {
        String base = "dual/" + UUID.randomUUID() + "/";
        SourceConfig a = config(MqttSourceSettings.V311, base + "+", null, "data2flow-ingress-it-dual-0");
        assertThat(connector.scaling(a)).isEqualTo(ScalingMode.DUAL_ACTIVE);
        RecordingRawSink sinkA = start(a, "it-0");
        RecordingRawSink sinkB = start(config(MqttSourceSettings.V311, base + "+", null, "data2flow-ingress-it-dual-1"), "it-1");
        publish(base, 1_000);
        await().atMost(Duration.ofSeconds(60)).until(() -> sinkA.writtenCount() >= 1_000 && sinkB.writtenCount() >= 1_000);
        Set<String> keysA = new HashSet<>(sinkA.written().stream().map(RawEnvelope::dedupKey).toList());
        Set<String> keysB = new HashSet<>(sinkB.written().stream().map(RawEnvelope::dedupKey).toList());
        assertThat(keysA).hasSize(1_000).isEqualTo(keysB);
        Set<java.util.UUID> ids = new HashSet<>();
        sinkA.written().forEach(e -> ids.add(e.messageId()));
        sinkB.written().forEach(e -> ids.add(e.messageId()));
        assertThat(ids).as("messageId는 인스턴스마다 다르다").hasSize(2_000);
    }
}
