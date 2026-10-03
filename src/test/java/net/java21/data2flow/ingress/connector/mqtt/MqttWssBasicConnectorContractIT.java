package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.support.NginxEdge;
import net.java21.data2flow.ingress.support.TestPki;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * DSC-09.02 TC-DSC-239: MQTT over WSS + HTTP Basic 헤더(iot-data.java21.net 방식: nginx TLS·Basic 인증 → Mosquitto WebSocket)로
 * 계약 키트 공통 시나리오를 통과한다. 설정은 connectors.md §3 예시 모양({@code auth{type: ws-header, scheme: Basic}})이고, 비밀값은
 * .env {@code MQTT_BASIC_AUTH}와 같은 {@code 사용자:비밀번호} 형식이다(DSC-01.05).
 */
class MqttWssBasicConnectorContractIT extends AbstractMqttContractIT {

    static final TestPki PKI = TestPki.create();
    static final String USER = "iot-data";
    static final String PASSWORD = "test-only-password";
    private static GenericContainer<?> nginx;

    static synchronized GenericContainer<?> nginx() {
        if (nginx == null) {
            nginx = NginxEdge.start(PKI, proxy(true).port(), 0, USER, PASSWORD);
        }
        return nginx;
    }

    @AfterAll
    static void stopNginx() {
        if (nginx != null) {
            nginx.stop();
            nginx = null;
        }
    }

    @Override
    protected String url() {
        return "wss://localhost:" + nginx().getMappedPort(NginxEdge.WSS_PORT) + "/mqtt";
    }

    @Override
    protected String version() {
        return MqttSourceSettings.V5;
    }

    @Override
    protected void customize(ObjectNode config) {
        config.putObject("auth").put("type", "ws-header").put("scheme", "Basic");
    }

    @Override
    protected Map<String, Secret> secrets() {
        return Map.of("HEADER_VALUE", Secret.of(USER + ":" + PASSWORD), "CA_CERT", Secret.of(PKI.caPem()));
    }

    @Override
    protected String clientId() {
        return "data2flow-ingress-it-wss-0";
    }
}
