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
 * ING-01.03 TC-ING-021(reliability-and-ha.md §4.1): SIGTERM을 받으면 새 메시지는 넘기지 않고, 기록 중인 메시지의 confirm을 기다려(최대 20초, confirm 지연 1초)
 * 확인(PUBACK)까지 보낸 뒤 세션을 남긴 채 끊는다. 종료 중 받은 것은 확인하지 않았으므로 다음 실행(같은 client-id)이 받는다 → 유실 0.
 * 기록한 메시지는 모두 확인했으므로 다시 받는 중복도 없다.
 */
class GracefulShutdownIT {

    static final int TOTAL = 600;
    static final long SOURCE = 23;

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
        p.put("data2flow.ingress.developer", "gs");
        IngressProcess process = IngressProcess.start("ingress-gs-0", p);
        processes.add(process);
        return process;
    }

    @Test
    @DisplayName("ING-01.03 TC-ING-021 SIGTERM → 기록 중 메시지 confirm·확인 후 종료, 종료 중 수신분 유실 0, 확인한 것은 다시 받지 않음")
    void sigtermDrainsInFlight() throws Exception {
        GenericContainer<?> broker = MqttTestBroker.shared();
        mqtt = new MqttAckCountingProxy(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), false);
        stream = RabbitTestBroker.streamProxy("stream-gs");
        String app = UUID.randomUUID().toString();
        core.sources("1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", "tcp://localhost:" + mqtt.port(),
                "application/" + app + "/device/+/event/up", null) + "]");

        IngressProcess first = start().awaitConnected(Duration.ofMinutes(2));
        warmUp(broker, app);
        stream.latency(1000);
        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment());
             LoadGenerator load = new LoadGenerator(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), app)) {
            long ackBase = mqtt.acknowledgedCount();   // 예열 메시지 확인 수
            load.start(200, TOTAL);
            await().atMost(Duration.ofSeconds(30)).until(() -> load.sent() >= 200);
            long started = System.nanoTime();
            int exit = first.terminate(Duration.ofSeconds(60));
            long shutdownMillis = (System.nanoTime() - started) / 1_000_000;
            long acked = mqtt.acknowledgedCount() - ackBase;
            await().atMost(Duration.ofSeconds(30)).until(load::done);

            long writtenByFirst = reader.envelopes().stream().filter(e -> load.seqOf(e.payload()) >= 0).count();
            assertThat(exit).as("정상 종료 코드(SIGTERM 143 또는 0)").isIn(0, 143);
            assertThat(shutdownMillis).as("기록 중 메시지 confirm(1초 지연)을 기다렸다").isGreaterThanOrEqualTo(1000);
            assertThat(writtenByFirst).as("종료 전 기록·확인한 메시지가 있다").isGreaterThan(0);
            assertThat(acked).as("기록이 끝난 메시지는 모두 확인했다(drain)").isEqualTo(writtenByFirst);

            stream.removeToxics();
            start().awaitConnected(Duration.ofMinutes(2));
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    LosslessReport.of("probe", TOTAL, reader.envelopes(), load).lost() == 0);
            LosslessReport report = LosslessReport.of("TC-ING-021 graceful shutdown", TOTAL, reader.envelopes(), load)
                    .write("writtenAndAckedBeforeExit=" + writtenByFirst + ", shutdownMs=" + shutdownMillis);
            assertThat(report.lost()).isZero();
            assertThat(report.dupAfterDedup()).isZero();
            assertThat(report.rawDuplicates()).as("확인한 메시지는 다시 받지 않는다").isZero();
        }
    }
}
