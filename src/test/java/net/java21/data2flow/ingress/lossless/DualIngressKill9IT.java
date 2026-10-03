package net.java21.data2flow.ingress.lossless;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.IngressProcess;
import net.java21.data2flow.ingress.support.LoadGenerator;
import net.java21.data2flow.ingress.support.LosslessReport;
import net.java21.data2flow.ingress.support.MqttTestBroker;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * NFR-02.09 TC-NFR-022(AT-NFR-06.3) 이중 수신: ingress 2개가 서로 다른 client-id로 같은 토픽을 동시에 구독하는 중(ADR-015, DUAL_ACTIVE)
 * 초당 200건 부하에서 한 개를 {@code kill -9} 하면 수신 공백 0, 유실 0, (dedupKey 중복 제거 후) 중복 0.
 *
 * <p>ingress는 실제 별도 JVM 프로세스로 띄우고 {@code Process.destroyForcibly()}(SIGKILL)로 죽인다. 죽은 쪽을 같은 client-id로 다시 띄우면
 * 영속 세션에 쌓인 메시지를 이어 받는다(TC-ING-017과 같은 원리). Mosquitto·RabbitMQ는 Testcontainers(공용 인프라 미사용).
 */
class DualIngressKill9IT {

    static final int RATE = 200;
    static final int TOTAL = 4000;          // 20초
    static final int KILL_AFTER = 1600;     // 8초 시점
    static final long SOURCE = 21;

    private final CoreStub core = new CoreStub();
    private final List<IngressProcess> processes = new java.util.ArrayList<>();

    @AfterEach
    void cleanUp() {
        processes.forEach(IngressProcess::close);
        core.close();
    }

    Map<String, String> props() {
        Map<String, String> p = new HashMap<>(RabbitTestBroker.properties());
        p.put("data2flow.ingress.core-uri", core.uri());
        p.put("data2flow.ingress.developer", "dual");
        return p;
    }

    IngressProcess start(String instanceId) {
        IngressProcess p = IngressProcess.start(instanceId, props());
        processes.add(p);
        return p;
    }

    @Test
    @DisplayName("NFR-02.09 TC-NFR-022 초당 200건 중 ingress 1대 kill -9 → 수신 공백 0, 유실 0, 중복(중복 제거 후) 0")
    void killOneOfTwoIngress() {
        GenericContainer<?> broker = MqttTestBroker.shared();
        String app = UUID.randomUUID().toString();
        String url = "tcp://" + broker.getHost() + ":" + broker.getMappedPort(MqttTestBroker.MQTT_PORT);
        core.sources("1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", url, "application/" + app + "/device/+/event/up", null) + "]");

        IngressProcess a = start("ingress-dual-0");
        IngressProcess b = start("ingress-dual-1");
        a.awaitConnected(Duration.ofMinutes(2));
        b.awaitConnected(Duration.ofMinutes(2));

        try (RawStreamReader reader = new RawStreamReader(RabbitTestBroker.environment());
             LoadGenerator load = new LoadGenerator(broker.getHost(), broker.getMappedPort(MqttTestBroker.MQTT_PORT), app)) {
            load.start(RATE, TOTAL);
            await().atMost(Duration.ofSeconds(30)).until(() -> load.sent() >= KILL_AFTER);
            int killedAt = load.sent();
            a.kill9();
            assertThat(a.alive()).isFalse();

            await().atMost(Duration.ofSeconds(60)).until(load::done);
            assertThat(load.failed()).isZero();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    LosslessReport.of("probe", TOTAL, reader.envelopes(), load).lost() == 0);

            List<RawEnvelope> all = reader.envelopes();
            LosslessReport report = LosslessReport.of("NFR-02.09 dual ingress kill -9", TOTAL, all, load)
                    .write("rate=" + RATE + "/s, killedAtSeq=" + killedAt);
            assertThat(report.missing()).as("유실된 seq").isEmpty();
            assertThat(report.lost()).isZero();
            assertThat(report.dupAfterDedup()).as("dedupKey 중복 제거 후 남는 중복").isZero();

            // 수신 공백 0: 죽인 뒤 보낸 메시지는 살아 있는 ingress가 모두 받았다
            Set<Integer> fromB = new HashSet<>();
            all.stream().filter(e -> e.ingressInstance().equals("ingress-dual-1"))
                    .forEach(e -> fromB.add(load.seqOf(e.payload())));
            for (int s = killedAt; s < TOTAL; s++) {
                assertThat(fromB).as("kill 뒤 seq %d를 살아 있는 ingress가 받았는가", s).contains(s);
            }

            // 죽은 인스턴스를 같은 client-id로 다시 띄우면 영속 세션에 쌓인 메시지를 이어 받는다(중복은 dedupKey로 걸러짐)
            int beforeRestart = (int) all.stream().filter(e -> e.ingressInstance().equals("ingress-dual-0")).count();
            IngressProcess a2 = start("ingress-dual-0");
            a2.awaitConnected(Duration.ofMinutes(2));
            await().atMost(Duration.ofSeconds(60)).until(() -> reader.envelopes().stream()
                    .filter(e -> e.ingressInstance().equals("ingress-dual-0") && load.seqOf(e.payload()) >= 0)
                    .map(e -> load.seqOf(e.payload())).distinct().count() >= TOTAL - 1);
            LosslessReport after = LosslessReport.of("NFR-02.09 after restart of killed instance", TOTAL,
                    reader.envelopes(), load).write("killedInstanceRecordsBeforeKill=" + beforeRestart);
            assertThat(after.lost()).isZero();
            assertThat(after.dupAfterDedup()).isZero();
        }
    }
}
