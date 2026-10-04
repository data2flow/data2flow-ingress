package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import tools.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * DSC-09.02 TC-DSC-325: The Things Stack v3 프리셋(사용자 {@code app@tenant}, API 키, 토픽 {@code v3/{사용자}/devices/+/up})이 시험
 * Mosquitto를 상대로 계약 키트 공통 시나리오를 통과한다. 실제 TTS 계정이 없어 같은 토픽 모양의 브로커로 시험한다(ADR-040).
 */
class TheThingsStackConnectorContractIT extends AbstractMqttPresetContractIT {

    @Override
    protected MqttPresetConnector preset(MqttSourceConnector mqtt) {
        return MqttPresets.theThingsStack(mqtt);
    }

    @Override
    protected ObjectNode presetConfig(String proxyUrl) {
        return JSON.createObjectNode().put("host", "eu1.cloud.thethings.network").put("url", proxyUrl)
                .put("applicationId", "kit-app").put("tenantId", "ttn");
    }

    @Override
    protected String publishTopic() {
        return "v3/kit-app@ttn/devices/eui-0004a30b001c0530/up";
    }

    @Override
    protected Map<String, Secret> secrets() {
        return Map.of("PASSWORD", Secret.of("NNSXS.KITAPIKEY"));
    }

    @Override
    protected long sourceId() {
        return 41;
    }
}
