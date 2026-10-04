package net.java21.data2flow.ingress.connector.common;

/**
 * {@code RawSink.write}가 실패로 끝났다(스트림 confirm 실패·시간 초과). 커넥터는 상대에게 확인하지 않고(nack·커서 미저장) 같은 메시지를
 * 다시 받는다(DSC-09.03, TC-ING-016). 연결은 끊지 않는다.
 */
public class WriteFailedException extends Exception {

    public WriteFailedException(Throwable cause) {
        super("원본 기록 실패: " + cause, cause);
    }
}
