package net.java21.data2flow.ingress.connectiontest.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import tools.jackson.databind.JsonNode;

/**
 * API-DSC-51 요청(API-DSC-57 요청과 같음). 비밀값은 저장하지 않고 이 요청 안에서만 쓴다.
 *
 * @param organizationId 조직(동시 테스트 한도 단위)
 * @param type           소스 유형(MQTT_SUBSCRIBE·PLATFORM_BROKER·CONNECTOR, API 표기 MQTT도 받음)
 * @param connectorKey   CONNECTOR 유형의 커넥터 키
 * @param config         연결 설정(토픽 포함)
 * @param topics         설정 밖에 따로 온 토픽 목록(API-DSC-02 모양). 있으면 설정에 합친다
 * @param secrets        비밀값: {@code {종류: 값}} 또는 {@code [{kind, value}]}
 * @param timeoutSec     제한 시간(1~30초, 기본 15초)
 */
public record SourceTestRequest(@NotNull @Min(1) Long organizationId, @NotBlank String type, String connectorKey,
                                @NotNull JsonNode config, JsonNode topics, JsonNode secrets, Integer timeoutSec) {

    @Override
    public String toString() {
        return "SourceTestRequest[organizationId=" + organizationId + ", type=" + type + ", connectorKey=" + connectorKey
                + ", timeoutSec=" + timeoutSec + "]";
    }
}
