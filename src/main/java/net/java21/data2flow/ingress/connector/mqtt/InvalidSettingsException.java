package net.java21.data2flow.ingress.connector.mqtt;

/**
 * 소스 설정으로는 연결할 수 없다(SOURCE_CONFIG_INVALID·SOURCE_SECRET_REQUIRED·SOURCE_AUTH_UNSUPPORTED).
 * 메시지에는 필드 이름만 담고 값(비밀값일 수 있음)은 담지 않는다.
 */
public class InvalidSettingsException extends IllegalArgumentException {

    public enum Reason { CONFIG_INVALID, SECRET_REQUIRED, AUTH_UNSUPPORTED }

    private final Reason reason;
    private final String field;

    public InvalidSettingsException(Reason reason, String field) {
        super(reason + ": " + field);
        this.reason = reason;
        this.field = field;
    }

    public static InvalidSettingsException config(String field) {
        return new InvalidSettingsException(Reason.CONFIG_INVALID, field);
    }

    public Reason reason() {
        return reason;
    }

    public String field() {
        return field;
    }
}
