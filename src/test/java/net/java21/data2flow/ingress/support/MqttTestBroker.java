package net.java21.data2flow.ingress.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.time.Duration;

/**
 * 테스트 전용 Mosquitto 2.0(실제 iot-mosquitto와 같은 계열). MQTT 1883, WebSocket 9001. 공용 브로커 {@code iot-data.java21.net}에는
 * 절대 붙지 않는다(CLAUDE.md §5). 세션 큐 한도는 시험 중 유실이 브로커 한도 때문에 생기지 않게 크게 둔다
 * (실제 브로커의 {@code max_queued_messages}는 deployment.md §11에서 확인할 항목).
 */
public final class MqttTestBroker {

    public static final int MQTT_PORT = 1883;
    public static final int WS_PORT = 9001;
    private static final String ACL = "topic read allowed/#\n";
    private static final String CONFIG = """
            listener 1883
            protocol mqtt
            listener 9001
            protocol websockets
            allow_anonymous true
            persistence false
            max_queued_messages 200000
            max_inflight_messages 100
            persistent_client_expiration 1h
            log_type error
            log_type warning
            """;

    private static GenericContainer<?> shared;

    private MqttTestBroker() {
    }

    /** JVM 안에서 하나를 함께 쓴다(Ryuk가 정리) */
    public static synchronized GenericContainer<?> shared() {
        if (shared == null) {
            shared = create();
            shared.start();
        }
        return shared;
    }

    public static GenericContainer<?> create() {
        return new GenericContainer<>("eclipse-mosquitto:2.0")
                .withExposedPorts(MQTT_PORT, WS_PORT)
                .withCopyToContainer(Transferable.of(CONFIG), "/mosquitto/config/mosquitto.conf")
                .waitingFor(Wait.forListeningPorts(MQTT_PORT).withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * 구독 권한이 {@code allowed/#}뿐인 별도 브로커(구독 거부 시험, TC-DSC-303). per_listener_settings로 한 브로커에 섞으면 Mosquitto 2.0이
     * 오프라인 영속 세션에 메시지를 쌓지 않으므로 따로 띄운다.
     */
    public static GenericContainer<?> createAclBroker() {
        return new GenericContainer<>("eclipse-mosquitto:2.0")
                .withExposedPorts(MQTT_PORT)
                .withCopyToContainer(Transferable.of("listener 1883\nallow_anonymous true\nacl_file /mosquitto/config/acl\n"),
                        "/mosquitto/config/mosquitto.conf")
                .withCopyToContainer(Transferable.of(ACL, 0600), "/mosquitto/config/acl")
                .waitingFor(Wait.forListeningPorts(MQTT_PORT).withStartupTimeout(Duration.ofMinutes(2)));
    }

    public static String host(GenericContainer<?> broker) {
        return broker.getHost();
    }
}
