package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.support.NginxEdge;
import net.java21.data2flow.ingress.support.TestPki;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * DSC-09.02 TC-DSC-240: MQTT mTLS(X.509 클라이언트 인증서, AWS IoT 방식)로 계약 키트 공통 시나리오를 통과한다(DSC-01.05).
 * TLS 종단은 클라이언트 인증서를 검증하는 nginx stream 서버이고 그 뒤는 Mosquitto 1883이다.
 */
class MqttMtlsConnectorContractIT extends AbstractMqttContractIT {

    static final TestPki PKI = TestPki.create();
    private static GenericContainer<?> nginx;

    static synchronized GenericContainer<?> nginx() {
        if (nginx == null) {
            nginx = NginxEdge.start(PKI, 0, proxy(false).port(), "unused", "unused");
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
        return "ssl://localhost:" + nginx().getMappedPort(NginxEdge.MTLS_PORT);
    }

    @Override
    protected String version() {
        return MqttSourceSettings.V311;
    }

    @Override
    protected void customize(ObjectNode config) {
        config.put("auth", "MTLS");
    }

    @Override
    protected Map<String, Secret> secrets() {
        return Map.of("CLIENT_CERT", Secret.of(PKI.clientCertPem()), "CLIENT_KEY", Secret.of(PKI.clientKeyPem()),
                "CA_CERT", Secret.of(PKI.caPem()));
    }

    @Override
    protected String clientId() {
        return "data2flow-ingress-it-mtls-0";
    }
}
