package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSC-09.02·09.05 TC-DSC-325: Azure IoT Hub 프리셋(사용자 이름 형식, SAS 토큰 생성, 클라우드→장치 토픽)이 시험 Mosquitto를 상대로 계약 키트
 * 공통 시나리오를 통과한다. 실제 허브가 없어 SAS 검증은 하지 않는 브로커로 시험한다(ADR-040).
 */
class AzureIotHubConnectorContractIT extends AbstractMqttPresetContractIT {

    static final String KEY = Base64.getEncoder().encodeToString("kit-device-symmetric-key-000000".getBytes());

    @Override
    protected MqttPresetConnector preset(MqttSourceConnector mqtt) {
        return MqttPresets.azureIotHub(mqtt, Clock.systemUTC());
    }

    @Override
    protected ObjectNode presetConfig(String proxyUrl) {
        return JSON.createObjectNode().put("hubName", "kit-hub").put("deviceId", "kit-device").put("url", proxyUrl);
    }

    @Override
    protected String publishTopic() {
        return "devices/kit-device/messages/devicebound/%24.to=kit";
    }

    @Override
    protected Map<String, Secret> secrets() {
        return Map.of("SAS_KEY", Secret.of(KEY));
    }

    @Override
    protected long sourceId() {
        return 42;
    }

    @Test
    @DisplayName("DSC-09.05 BR-DSC-27 SAS 토큰은 sr·sig·se를 담고 같은 입력이면 같은 서명이다")
    void sasTokenShape() {
        String t = MqttPresets.sasToken("kit-hub.azure-devices.net/devices/kit-device", KEY, 1_800_000_000L);
        assertThat(t).startsWith("SharedAccessSignature sr=kit-hub.azure-devices.net%2Fdevices%2Fkit-device&sig=")
                .endsWith("&se=1800000000");
        assertThat(MqttPresets.sasToken("kit-hub.azure-devices.net/devices/kit-device", KEY, 1_800_000_000L)).isEqualTo(t);
    }
}
