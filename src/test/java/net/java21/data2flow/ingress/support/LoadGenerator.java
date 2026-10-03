package net.java21.data2flow.ingress.support;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;

/**
 * 테스트 브로커(Testcontainers Mosquitto)에 초당 N건 QoS 1로 발행하는 부하 발생기(TC-NFR-022: 초당 200건). payload에는 {@code seq}(유실·중복
 * 판정 기준)와 실행 ID가 있다. 기기 10대의 ChirpStack 토픽으로 나눠 보낸다. <b>공용 브로커에는 발행하지 않는다</b>(CLAUDE.md §5).
 */
public final class LoadGenerator implements AutoCloseable {

    private final Mqtt5AsyncClient client;
    private final String run = UUID.randomUUID().toString().substring(0, 8);
    private final String topicPrefix;
    private final AtomicInteger next = new AtomicInteger();
    private final Set<Integer> acked = ConcurrentHashMap.newKeySet();
    private final AtomicInteger failed = new AtomicInteger();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("load").factory());
    private ScheduledFuture<?> task;
    private volatile int limit;

    public LoadGenerator(String host, int port, String application) {
        if (host.contains("java21.net")) {
            throw new IllegalArgumentException("공용 브로커에는 발행하지 않습니다(CLAUDE.md §5)");
        }
        this.topicPrefix = "application/" + application + "/device/";
        this.client = MqttClient.builder().useMqttVersion5().identifier("load-" + run).serverHost(host).serverPort(port)
                .buildAsync();
        this.client.connect().join();
    }

    /** 초당 ratePerSecond건으로 total건을 보낸다(비동기) */
    public void start(int ratePerSecond, int total) {
        this.limit = total;
        long periodMicros = 1_000_000L / ratePerSecond;
        task = scheduler.scheduleAtFixedRate(this::sendOne, 0, periodMicros, TimeUnit.MICROSECONDS);
    }

    private void sendOne() {
        int seq = next.getAndIncrement();
        if (seq >= limit) {
            task.cancel(false);
            return;
        }
        String device = String.format("dev%016x", seq % 10);
        byte[] payload = ("{\"run\":\"" + run + "\",\"seq\":" + seq + ",\"devEui\":\"" + device + "\"}")
                .getBytes(StandardCharsets.UTF_8);
        client.publishWith().topic(topicPrefix + device + "/event/up").qos(MqttQos.AT_LEAST_ONCE).payload(payload).send()
                .whenComplete((r, e) -> {
                    if (e == null) {
                        acked.add(seq);
                    } else {
                        failed.incrementAndGet();
                    }
                });
    }

    /** 지금까지 보낸(발행 시도한) 수 */
    public int sent() {
        return Math.min(next.get(), limit);
    }

    /** 브로커가 받았다고 확인한 수 */
    public int brokerAcked() {
        return acked.size();
    }

    public int failed() {
        return failed.get();
    }

    public String run() {
        return run;
    }

    public boolean done() {
        return next.get() >= limit && acked.size() + failed.get() >= limit;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        client.disconnect();
    }

    /** payload에서 seq를 꺼낸다. 이 실행의 메시지가 아니면 -1 */
    public int seqOf(byte[] payload) {
        String s = new String(payload, StandardCharsets.UTF_8);
        if (!s.contains("\"run\":\"" + run + "\"")) {
            return -1;
        }
        return Integer.parseInt(s.replaceAll(".*\"seq\":(\\d+).*", "$1"));
    }
}
