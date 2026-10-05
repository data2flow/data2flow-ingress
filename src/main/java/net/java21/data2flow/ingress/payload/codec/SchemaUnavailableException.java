package net.java21.data2flow.ingress.payload.codec;

/**
 * 스키마를 지금은 가져올 수 없다(core·스키마 레지스트리 접속 실패·5xx·시간 초과). 기록을 실패로 끝내 상대에게 확인하지 않으므로
 * 상대가 다시 보낸다(무손실, DSC-09.03). 스키마가 없다고 확정된 경우(404)는 {@link PayloadDecodeException}이다.
 */
public class SchemaUnavailableException extends RuntimeException {

    public SchemaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
