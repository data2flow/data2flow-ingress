package net.java21.data2flow.ingress.connector.domain;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionErrorsTest {

    @Test
    @DisplayName("DSC-02.05 실패 원인을 인증·DNS·TLS·타임아웃·거부로 나눈다(error_kind)")
    void classifiesCauses() {
        assertThat(ConnectionErrors.classify(new CompletionException(new UnknownHostException("x")))).isEqualTo(ConnectionErrorKind.DNS);
        assertThat(ConnectionErrors.classify(new SSLHandshakeException("PKIX path"))).isEqualTo(ConnectionErrorKind.TLS);
        assertThat(ConnectionErrors.classify(new ConnectException("Connection refused"))).isEqualTo(ConnectionErrorKind.REFUSED);
        assertThat(ConnectionErrors.classify(new SocketTimeoutException("read"))).isEqualTo(ConnectionErrorKind.TIMEOUT);
        assertThat(ConnectionErrors.classify(new IllegalStateException("connection timed out"))).isEqualTo(ConnectionErrorKind.TIMEOUT);
        assertThat(ConnectionErrors.classify(new IllegalStateException("Connection refused: localhost"))).isEqualTo(ConnectionErrorKind.REFUSED);
        assertThat(ConnectionErrors.classify(new FakeConnAckException("NOT_AUTHORIZED"))).isEqualTo(ConnectionErrorKind.AUTH);
        assertThat(ConnectionErrors.classify(new FakeConnAckException("UNSPECIFIED_ERROR"))).isEqualTo(ConnectionErrorKind.PROTOCOL);
        assertThat(ConnectionErrors.classify(new IllegalStateException("Invalid handshake response getStatus: 401 Unauthorized")))
                .isEqualTo(ConnectionErrorKind.AUTH);
        assertThat(ConnectionErrors.classify(new IllegalStateException("Invalid handshake response getStatus: 502")))
                .isEqualTo(ConnectionErrorKind.PROTOCOL);
        assertThat(ConnectionErrors.classify(new IllegalStateException("boom"))).isEqualTo(ConnectionErrorKind.OTHER);
    }

    @Test
    @DisplayName("DSC 3.2 인증·TLS만 확정 실패로 본다")
    void fatalKinds() {
        assertThat(ConnectionErrors.isFatal(ConnectionErrorKind.AUTH)).isTrue();
        assertThat(ConnectionErrors.isFatal(ConnectionErrorKind.TLS)).isTrue();
        assertThat(ConnectionErrors.isFatal(ConnectionErrorKind.TIMEOUT)).isFalse();
    }

    @Test
    @DisplayName("설명은 근본 원인 이름과 문구, 500자 이하")
    void describeIsBounded() {
        String longText = "x".repeat(800);
        assertThat(ConnectionErrors.describe(new RuntimeException(new IllegalStateException(longText)))).hasSize(500)
                .startsWith("IllegalStateException: ");
    }

    static final class FakeConnAckException extends RuntimeException {
        FakeConnAckException(String m) {
            super(m);
        }
    }
}
