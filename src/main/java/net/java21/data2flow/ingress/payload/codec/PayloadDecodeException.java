package net.java21.data2flow.ingress.payload.codec;

/**
 * payload를 지정한 형식·압축으로 풀 수 없다(DSC-09.07 예외 흐름 1a). 메시지는 {@code DECODE_ERROR}로 원본과 함께 기록되고 확인(ACK)은
 * 정상대로 보낸다 — 다시 받아도 같은 결과라서다. 일시적인 문제(스키마 저장소 접속 실패)는 {@link SchemaUnavailableException}이다.
 */
public class PayloadDecodeException extends Exception {

    public PayloadDecodeException(String message) {
        super(message);
    }

    public PayloadDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
