package net.java21.data2flow.ingress.support;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 커넥터 계약 IT의 상대 브로커: RabbitMQ 4(AMQP 0-9-1과 AMQP 1.0 기본 지원). 수집 경로가 쓰는 {@link RabbitTestBroker}(3.13 stream)와는
 * 다른 "외부 고객 브로커" 역할이다. 확인 수는 관리 API의 큐 통계(ready + unacked)로 센다(통계 주기 100ms).
 */
public final class QueueTestBroker {

    public static final int AMQP = 5672;
    public static final int HTTP = 15672;
    private static GenericContainer<?> shared;

    private QueueTestBroker() {
    }

    public static synchronized GenericContainer<?> shared() {
        if (shared == null) {
            shared = new GenericContainer<>("rabbitmq:4-management")
                    .withExposedPorts(AMQP, HTTP)
                    .withCopyToContainer(Transferable.of("""
                            collect_statistics_interval = 100
                            loopback_users = none
                            """), "/etc/rabbitmq/conf.d/90-test.conf")
                    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(3)));
            shared.start();
        }
        return shared;
    }

    public static String host() {
        return shared().getHost();
    }

    public static int amqpPort() {
        return shared().getMappedPort(AMQP);
    }

    public static Connection connect() throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host());
        f.setPort(amqpPort());
        return f.newConnection("contract-peer");
    }

    /** 큐를 만든다(시험 상대 준비. 커넥터는 큐를 만들지 않는다) */
    public static void declareQueue(String queue, String type) throws Exception {
        try (Connection c = connect(); Channel ch = c.createChannel()) {
            ch.queueDeclare(queue, true, false, false, Map.of("x-queue-type", type));
        }
    }

    /** 순서대로 영속 메시지를 보낸다 */
    public static void publish(String queue, List<byte[]> payloads) throws Exception {
        try (Connection c = connect(); Channel ch = c.createChannel()) {
            ch.confirmSelect();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().deliveryMode(2).build();
            for (byte[] p : payloads) {
                ch.basicPublish("", queue, props, p);
            }
            ch.waitForConfirmsOrDie(30_000);
        }
    }

    /**
     * 큐에 남은 메시지 수(ready + unacked). 관리 API 통계는 늦게 반영되므로 큐 프로세스에서 바로 읽는 {@code rabbitmqctl}을 쓴다.
     */
    public static long remaining(String queue) {
        try {
            var r = shared().execInContainer("rabbitmqctl", "list_queues", "-q", "--no-table-headers", "name",
                    "messages_ready", "messages_unacknowledged");
            for (String line : r.getStdout().split("\\R")) {
                String[] cols = line.trim().split("\\s+");
                if (cols.length == 3 && cols[0].equals(queue)) {
                    return Long.parseLong(cols[1]) + Long.parseLong(cols[2]);
                }
            }
            throw new IllegalStateException("큐가 없습니다: " + queue + " / " + r.getStdout() + r.getStderr());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
