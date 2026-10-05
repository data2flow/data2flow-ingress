package net.java21.data2flow.ingress.payload.dto;

import java.util.List;

/**
 * API-DSC-82 응답(core는 API-DSC-59 응답 {@code messageTypes[]}로 그대로 쓴다).
 *
 * @param format       PROTOBUF 또는 AVRO
 * @param messageTypes Protobuf 메시지 전체 이름들, Avro는 최상위 스키마 이름 하나
 * @param messageType  요청한 타입(확인됨) 또는 하나뿐일 때 그 타입. 정할 수 없으면 null
 */
public record SchemaInspectResponse(String format, List<String> messageTypes, String messageType) {
}
