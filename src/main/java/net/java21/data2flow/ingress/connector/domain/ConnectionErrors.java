package net.java21.data2flow.ingress.connector.domain;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

/**
 * 연결 실패를 종류(DSC domain-model §2.5 {@code error_kind})로 나눈다. 화면은 종류마다 안내 문구를 고르고,
 * 인증·TLS 오류는 다시 해도 실패가 확실하므로 5회 뒤 ERROR로 둔다(§3.2).
 */
public final class ConnectionErrors {

    private ConnectionErrors() {
    }

    public static ConnectionErrorKind classify(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof UnknownHostException) {
                return ConnectionErrorKind.DNS;
            }
            if (t instanceof SSLException || t instanceof java.security.cert.CertificateException) {
                return ConnectionErrorKind.TLS;
            }
            if (t instanceof ConnectException || t instanceof NoRouteToHostException) {
                return ConnectionErrorKind.REFUSED;
            }
            if (t instanceof SocketTimeoutException || t instanceof TimeoutException) {
                return ConnectionErrorKind.TIMEOUT;
            }
            String name = t.getClass().getSimpleName();
            String message = String.valueOf(t.getMessage()).toLowerCase(Locale.ROOT);
            if (name.contains("ConnAck")) {
                return message.contains("not_authorized") || message.contains("bad_user_name")
                        || message.contains("not authorized") || message.contains("bad user name")
                        ? ConnectionErrorKind.AUTH : ConnectionErrorKind.PROTOCOL;
            }
            if (name.contains("WebSocketHandshake") || message.contains("invalid handshake response")) {
                return message.contains("401") || message.contains("403")
                        ? ConnectionErrorKind.AUTH : ConnectionErrorKind.PROTOCOL;
            }
            if (message.contains("timed out") || message.contains("timeout")) {
                return ConnectionErrorKind.TIMEOUT;
            }
            if (message.contains("connection refused")) {
                return ConnectionErrorKind.REFUSED;
            }
        }
        return ConnectionErrorKind.OTHER;
    }

    /** 다시 해도 실패가 확실한 오류인가(인증·TLS) */
    public static boolean isFatal(ConnectionErrorKind kind) {
        return kind == ConnectionErrorKind.AUTH || kind == ConnectionErrorKind.TLS;
    }

    /** 화면용 설명: 비밀값 없이 500자 이하 */
    public static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String text = root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
        return text.length() > 500 ? text.substring(0, 500) : text;
    }
}
