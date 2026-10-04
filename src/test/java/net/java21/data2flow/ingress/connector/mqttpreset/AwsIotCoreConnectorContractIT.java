package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import net.java21.data2flow.ingress.support.NginxEdge;
import net.java21.data2flow.ingress.support.TestPki;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * DSC-09.02·09.05 TC-DSC-325: AWS IoT Core 프리셋(X.509 클라이언트 인증서 mTLS, 8883)이 시험 PKI로 클라이언트 인증서를 요구하는 TLS 앞단
 * (nginx stream) + Mosquitto를 상대로 계약 키트 공통 시나리오를 통과한다. 실제 AWS 계정이 없어 같은 인증 방식의 브로커로 시험한다(ADR-040).
 */
class AwsIotCoreConnectorContractIT extends AbstractMqttPresetContractIT {

    static final TestPki PKI = TestPki.create();
    private static GenericContainer<?> nginx;

    static synchronized GenericContainer<?> nginx() {
        if (nginx == null) {
            nginx = NginxEdge.start(PKI, 0, proxy().port(), "unused", "unused");
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
    protected String proxyUrl() {
        return "ssl://localhost:" + nginx().getMappedPort(NginxEdge.MTLS_PORT);
    }

    @Override
    protected MqttPresetConnector preset(MqttSourceConnector mqtt) {
        return MqttPresets.awsIot(mqtt);
    }

    @Override
    protected ObjectNode presetConfig(String proxyUrl) {
        ObjectNode c = JSON.createObjectNode().put("endpoint", "kit-ats.iot.ap-northeast-2.amazonaws.com").put("url", proxyUrl);
        c.putArray("topics").add("dt/kit/+/telemetry");
        return c;
    }

    @Override
    protected String publishTopic() {
        return "dt/kit/sensor-1/telemetry";
    }

    @Override
    protected Map<String, Secret> secrets() {
        return Map.of("CLIENT_CERT", Secret.of(PKI.clientCertPem()), "CLIENT_KEY", Secret.of(PKI.clientKeyPem()),
                "CA_CERT", Secret.of(PKI.caPem()));
    }

    @Override
    protected long sourceId() {
        return 43;
    }
}
