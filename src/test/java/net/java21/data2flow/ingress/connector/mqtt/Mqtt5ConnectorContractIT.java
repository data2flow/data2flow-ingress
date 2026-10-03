package net.java21.data2flow.ingress.connector.mqtt;

import tools.jackson.databind.node.ObjectNode;

/**
 * DSC-09.02 TC-DSC-238: MQTT 5.0 + 공유 구독({@code $share/kit/…})으로 계약 키트 공통 시나리오(연결·구독·1,000건 수신·RawEnvelope
 * 필드·기록 전 확인 금지 AT-DSC-18.1·기록 실패 재전송·일시정지/재개·상태 보고·닫기)를 통과한다.
 */
class Mqtt5ConnectorContractIT extends AbstractMqttContractIT {

    @Override
    protected String url() {
        return "tcp://localhost:" + proxy(false).port();
    }

    @Override
    protected String version() {
        return MqttSourceSettings.V5;
    }

    @Override
    protected void customize(ObjectNode config) {
        config.put("sharedGroup", "kit");
        config.put("retainHandling", "DO_NOT_SEND");
    }

    @Override
    protected String clientId() {
        return "data2flow-ingress-it-v5-0";
    }
}
