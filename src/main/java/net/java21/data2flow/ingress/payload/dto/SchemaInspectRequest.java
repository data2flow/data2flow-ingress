package net.java21.data2flow.ingress.payload.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * API-DSC-82 요청: core가 API-DSC-59 업로드를 저장하기 전에 해석을 맡긴다.
 *
 * @param format      PROTOBUF 또는 AVRO
 * @param fileName    올린 파일 이름(.proto·.desc·.binpb·.avsc). 없으면 형식 기본
 * @param content     파일 내용(base64, 최대 1MiB 원본)
 * @param messageType 고른 Protobuf 메시지 타입(있으면 존재 확인)
 */
public record SchemaInspectRequest(
        @NotBlank @Pattern(regexp = "(?i)PROTOBUF|AVRO") String format,
        @Size(max = 255) String fileName,
        @NotBlank @Size(max = 1_400_000) String content,
        @Size(max = 255) String messageType) {
}
