package net.java21.data2flow.ingress.lossless;

import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.LoadGenerator;
import net.java21.data2flow.ingress.support.LosslessReport;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import net.java21.data2flow.ingress.stream.service.RawStreamWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ING-01.03 TC-ING-016, DSC-09.03 BR-DSC-24, AT-NFR-05.7(축소판): QoS 1 메시지는 스트림 publisher confirm <b>뒤에만</b> PUBACK한다.
 * RabbitMQ Stream 연결을 Toxiproxy로 끊은 동안 받은 10건은 PUBACK이 없고 스트림에도 없다. 연결을 되살리면 Mosquitto 재전송으로 10건이
 * 모두 {@code data2flow.raw}에 기록되고 그때 확인된다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"management.server.port=0", "data2flow.ingress.instance-id=ingress-ack-0",
                "data2flow.ingress.developer=ack", "data2flow.ingress.mqtt.confirm-timeout=3s"})
class MqttManualAckIT {

    static final long SOURCE = 24;
    static final CoreStub CORE = new CoreStub();
    static final RabbitTestBroker.StreamProxy STREAM = RabbitTestBroker.streamProxy("stream-ack");
    static final MqttAckCountingProxy MQTT;
    static final String APP = UUID.randomUUID().toString();

    static {
        GenericContainer<?> b = MqttTestBroker.shared();
        try {
            MQTT = new MqttAckCountingProxy(b.getHost(), b.getMappedPort(MqttTestBroker.MQTT_PORT), false);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        CORE.sources("1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", "tcp://localhost:" + MQTT.port(),
                "application/" + APP + "/device/+/event/up", null) + "]");
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        RabbitTestBroker.properties(STREAM.host(), STREAM.port()).forEach((k, v) -> registry.add(k, () -> v));
        registry.add("data2flow.ingress.core-uri", CORE::uri);
    }

    @AfterAll
    static void tearDown() {
        STREAM.enabled(true);
        MQTT.close();
        CORE.close();
    }

    @Autowired
    SourceSupervisor supervisor;
    @Autowired
    RawStreamWriter writer;

    @Test
    @DisplayName("ING-01.03 TC-ING-016 RabbitMQ Stream이 끊긴 동안 받은 10건은 PUBACK 없음 → 복구 후 재전송으로 10건 모두 기록")
    void noPubackWithoutConfirm() {
        await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                .anyMatch(r -> r.session().status().state() == ConnectorState.CONNECTED));
        GenericContainer<?> b = MqttTestBroker.shared();
        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment());
             LoadGenerator warm = new LoadGenerator(b.getHost(), b.getMappedPort(MqttTestBroker.MQTT_PORT), APP);
             LoadGenerator load = new LoadGenerator(b.getHost(), b.getMappedPort(MqttTestBroker.MQTT_PORT), APP)) {
            warm.start(100, 10);
            await().atMost(Duration.ofSeconds(30)).until(() -> reader.envelopes().stream()
                    .filter(e -> warm.seqOf(e.payload()) >= 0).count() >= 10);
            long ackBase = MQTT.acknowledgedCount();

            STREAM.enabled(false);
            load.start(20, 10);
            await().atMost(Duration.ofSeconds(30)).until(load::done);
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8))
                    .until(() -> MQTT.acknowledgedCount() == ackBase);
            assertThat(reader.envelopes().stream().filter(e -> load.seqOf(e.payload()) >= 0)).isEmpty();

            STREAM.enabled(true);
            await().atMost(Duration.ofSeconds(120)).until(() ->
                    LosslessReport.of("probe", 10, reader.envelopes(), load).lost() == 0);
            await().atMost(Duration.ofSeconds(30)).until(() -> MQTT.acknowledgedCount() - ackBase >= 10);
            LosslessReport report = LosslessReport.of("TC-ING-016 manual ack while stream down", 10, reader.envelopes(), load)
                    .write("pubacksWhileDown=0, pubacksAfter=" + (MQTT.acknowledgedCount() - ackBase));
            assertThat(report.lost()).isZero();
            assertThat(report.dupAfterDedup()).isZero();
        }
    }
}
