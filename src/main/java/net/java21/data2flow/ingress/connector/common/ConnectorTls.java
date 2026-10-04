package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.ingress.connector.mqtt.TlsSupport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * 커넥터 공통 TLS 설정(DSC-09.06, BR-DSC-29). 설정 {@code tls{verify}}·{@code tlsInsecure}와 비밀값 CA_CERT·CLIENT_CERT·CLIENT_KEY(PEM)로
 * SSLContext를 만든다. 최소 TLS 1.2. 검증 끄기는 core가 개발 소스에만 허용한다.
 *
 * @param insecure 인증서 검증 끄기
 * @param caPem    추가로 믿을 CA. 없으면 JDK 기본
 * @param certPem  클라이언트 인증서(mTLS)
 * @param keyPem   클라이언트 개인 키(PKCS#8)
 */
public record ConnectorTls(boolean insecure, String caPem, String certPem, String keyPem) {

    public static ConnectorTls from(Cfg cfg) {
        boolean insecure = cfg.bool("tlsInsecure", cfg.node().path("tls").isObject()
                && !cfg.node().path("tls").path("verify").asBoolean(true));
        return new ConnectorTls(insecure, cfg.reveal("CA_CERT"), cfg.reveal("CLIENT_CERT"), cfg.reveal("CLIENT_KEY"));
    }

    public boolean mutual() {
        return certPem != null && keyPem != null;
    }

    public SSLContext context() throws GeneralSecurityException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf = TlsSupport.keyManagers(certPem, keyPem);
        TrustManagerFactory tmf = TlsSupport.trustManagers(caPem, insecure);
        ctx.init(kmf == null ? null : kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return ctx;
    }

    public TrustManagerFactory trustManagers() throws GeneralSecurityException {
        return TlsSupport.trustManagers(caPem, insecure);
    }

    public KeyManagerFactory keyManagers() throws GeneralSecurityException {
        return TlsSupport.keyManagers(certPem, keyPem);
    }
}
