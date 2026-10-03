package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.ingress.common.IngressProperties;

import java.time.Duration;

/**
 * MQTT 커넥터 동작 값(재연결 백오프, confirm 제한 시간, 역압 한도). 운영 기본값은 {@link IngressProperties.Mqtt}.
 *
 * @param connectTimeout     TCP·TLS·WebSocket·CONNACK 각 단계 제한 시간
 * @param testTimeout        연결 테스트 기본 제한 시간(BR-DSC-07: 15초). 요청 설정의 {@code testTimeoutSec}이 있으면 그 값
 */
public record MqttConnectorOptions(Duration backoffInitial, Duration backoffMax, int fatalAttempts,
                                   Duration fatalRetryInterval, Duration confirmTimeout, int receiveMaximum,
                                   int maxPayloadBytes, Duration connectTimeout, Duration testTimeout) {

    public static MqttConnectorOptions defaults() {
        return new MqttConnectorOptions(Duration.ofSeconds(1), Duration.ofSeconds(60), 5, Duration.ofMinutes(5),
                Duration.ofSeconds(10), 100, 512 * 1024, Duration.ofSeconds(10), Duration.ofSeconds(15));
    }

    public static MqttConnectorOptions from(IngressProperties p) {
        IngressProperties.Mqtt m = p.mqtt();
        return new MqttConnectorOptions(m.backoffInitial(), m.backoffMax(), m.fatalAttempts(), m.fatalRetryInterval(),
                m.confirmTimeout(), m.receiveMaximum(), m.maxPayloadBytes(), Duration.ofSeconds(10),
                p.connectionTest().defaultTimeout());
    }

    public MqttConnectorOptions withTestTimeout(Duration timeout) {
        return new MqttConnectorOptions(backoffInitial, backoffMax, fatalAttempts, fatalRetryInterval, confirmTimeout,
                receiveMaximum, maxPayloadBytes, connectTimeout, timeout);
    }

    public MqttConnectorOptions withBackoff(Duration initial, Duration max) {
        return new MqttConnectorOptions(initial, max, fatalAttempts, fatalRetryInterval, confirmTimeout,
                receiveMaximum, maxPayloadBytes, connectTimeout, testTimeout);
    }
}
