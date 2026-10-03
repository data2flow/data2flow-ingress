package net.java21.data2flow.ingress.lossless;

import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.IngressProcess;
import net.java21.data2flow.ingress.support.LoadGenerator;
import net.java21.data2flow.ingress.support.LosslessReport;
import net.java21.data2flow.ingress.support.MqttAckCountingProxy;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ING-01.03 TC-ING-017: ingress JVM이 메시지를 받은 뒤 스트림 confirm 전에 {@code kill -9} → 새 ingress가 같은 client-id로 영속 세션을
 * 이어받고 브로커가 다시 보낸 메시지를 스트림에 기록한다. 300건 seq 기준 유실 0(중복은 dedupKey가 같아 pipeline이 거른다).
 *
 * <p>confirm을 늦추려고 RabbitMQ Stream 앞 Toxiproxy에 응답 지연(3초)을 건다. MQTT는 PUBACK 계수 프록시를 거쳐 죽기 전 확인 수를 잰다.
 */
class IngressKillBeforeConfirmIT {

    static final int TOTAL = 300;
    static final long SOURCE = 22;

    private final CoreStub core = new CoreStub();
    private final List<IngressProcess> processes = new ArrayList<>();
    private MqttAckCountingProxy mqtt;
    private RabbitTestBroker.StreamProxy stream;

    @AfterEach
    void cleanUp() {
        processes.forEach(IngressProcess::close);
        core.close();
        if (mqtt != null) {
            mqtt.close();
        }
        if (stream != null) {
            stream.removeToxics();
        }
    }

    /**
     * 파티션별 생산자는 첫 메시지 때 만들어진다(RPC 몇 번). 지연을 걸기 전에 같은 기기 토픽 10개로 한 번씩 보내 만들어 둔다.
     * 실행 ID가 달라 집계에서 빠진다.
     */
    static void warmUp(GenericContainer<?> broker, String app) {
        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment());
             LoadGenerator warm = new LoadGenerator(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), app)) {
            warm.start(100, 10);
            await().atMost(Duration.ofSeconds(30)).until(() -> reader.envelopes().stream()
                    .filter(e -> warm.seqOf(e.payload()) >= 0).count() >= 10);
        }
    }

    IngressProcess start() {
        Map<String, String> p = new HashMap<>(RabbitTestBroker.properties(stream.host(), stream.port()));
        p.put("data2flow.ingress.core-uri", core.uri());
        p.put("data2flow.ingress.developer", "kill");
        IngressProcess process = IngressProcess.start("ingress-kill-0", p);
        processes.add(process);
        return process;
    }

    @Test
    @DisplayName("ING-01.03 TC-ING-017 confirm 전 kill -9 → 같은 client-id로 세션 재개, 300건 seq 기준 유실 0")
    void killBeforeConfirmThenResume() throws Exception {
        GenericContainer<?> broker = MqttTestBroker.shared();
        mqtt = new MqttAckCountingProxy(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), false);
        stream = RabbitTestBroker.streamProxy("stream-kill");
        String app = UUID.randomUUID().toString();
        core.sources("1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", "tcp://localhost:" + mqtt.port(),
                "application/" + app + "/device/+/event/up", null) + "]");

        IngressProcess first = start().awaitConnected(Duration.ofMinutes(2));
        warmUp(broker, app);
        stream.latency(3000);
        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment());
             LoadGenerator load = new LoadGenerator(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), app)) {
            load.start(200, TOTAL);
            await().atMost(Duration.ofSeconds(30)).until(() -> load.sent() >= 150);
            first.kill9();
            long acksAtKill = mqtt.acknowledgedCount();
            assertThat(acksAtKill).as("confirm이 3초 늦으므로 죽을 때까지 확인한 메시지가 보낸 것보다 적다").isLessThan(150);
            await().atMost(Duration.ofSeconds(30)).until(load::done);

            stream.removeToxics();
            start().awaitConnected(Duration.ofMinutes(2));
            await().atMost(Duration.ofSeconds(90)).until(() ->
                    LosslessReport.of("probe", TOTAL, reader.envelopes(), load).lost() == 0);
            LosslessReport report = LosslessReport.of("TC-ING-017 kill -9 before confirm", TOTAL, reader.envelopes(), load)
                    .write("pubacksBeforeKill=" + acksAtKill);
            assertThat(report.lost()).isZero();
            assertThat(report.dupAfterDedup()).isZero();
        }
    }
}
