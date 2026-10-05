package net.java21.data2flow.ingress.payload.schema;

import java.util.List;

/**
 * core가 보관한 업로드 스키마 한 건(API-DSC-81 응답, API-DSC-59로 올린 파일).
 *
 * @param schemaRef      참조(소스 설정 {@code payload.schemaRef}). 내용이 바뀌면 참조도 바뀐다(불변) — 그래서 ingress는 영구 캐시한다
 * @param organizationId 조직(소스 조직과 같아야 쓴다)
 * @param format         PROTOBUF 또는 AVRO
 * @param fileName       올린 파일 이름(.proto·.desc·.avsc)
 * @param content        파일 내용
 * @param messageTypes   core가 확인한 메시지 타입(참고용)
 */
public record PayloadSchema(String schemaRef, long organizationId, String format, String fileName, byte[] content,
                            List<String> messageTypes) {

    public PayloadSchema {
        messageTypes = messageTypes == null ? List.of() : List.copyOf(messageTypes);
    }
}
