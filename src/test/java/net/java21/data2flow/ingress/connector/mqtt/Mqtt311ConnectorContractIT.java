package net.java21.data2flow.ingress.connector.mqtt;

/**
 * DSC-09.02 TC-DSC-237: MQTT 3.1.1 tcp로 계약 키트 공통 시나리오(연결·구독·1,000건 수신·RawEnvelope 필드·기록 전 확인 금지
 * AT-DSC-18.1·기록 실패 재전송·일시정지/재개·상태 보고·닫기)를 통과한다. 3.1.1은 공유 구독이 없어 DUAL_ACTIVE(ADR-015)다.
 */
class Mqtt311ConnectorContractIT extends AbstractMqttContractIT {

    @Override
    protected String url() {
        return "tcp://localhost:" + proxy(false).port();
    }

    @Override
    protected String version() {
        return MqttSourceSettings.V311;
    }

    @Override
    protected String clientId() {
        return "data2flow-ingress-it-v311-0";
    }
}
