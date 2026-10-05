package net.java21.data2flow.ingress.payload.schema;

/**
 * 올린 스키마(.proto·.avsc·FileDescriptorSet)를 해석할 수 없다(SOURCE_SCHEMA_INVALID, API-DSC-59·82). 메시지는 사용자에게 보여 줄 원인이다
 * (줄 번호 포함, 스키마 내용 일부만).
 */
public class SchemaInvalidException extends RuntimeException {

    public SchemaInvalidException(String message) {
        super(message);
    }

    public SchemaInvalidException(String message, Throwable cause) {
        super(message, cause);
    }
}
