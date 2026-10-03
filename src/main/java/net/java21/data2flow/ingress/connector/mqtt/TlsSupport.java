package net.java21.data2flow.ingress.connector.mqtt;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

/**
 * TLS 설정(DSC-01.05 mTLS, BR-DSC-29: 최소 TLS 1.2, 검증 끄기는 개발 소스만). PEM 문자열에서 신뢰 저장소와 키 저장소를 만든다.
 */
public final class TlsSupport {

    /** 허용 프로토콜(TLS 1.2 이상) */
    public static final List<String> PROTOCOLS = List.of("TLSv1.3", "TLSv1.2");
    private static final char[] NO_PASSWORD = new char[0];

    private TlsSupport() {
    }

    /** CA PEM이 있으면 그 CA만 믿는다. 없으면 JDK 기본 신뢰 저장소. insecure면 검증하지 않는다 */
    public static TrustManagerFactory trustManagers(String caPem, boolean insecure) throws GeneralSecurityException {
        if (insecure) {
            return new InsecureTrustManagerFactory();
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (caPem == null || caPem.isBlank()) {
            tmf.init((KeyStore) null);
            return tmf;
        }
        KeyStore ks = emptyKeyStore();
        int i = 0;
        for (Certificate cert : certificates(caPem)) {
            ks.setCertificateEntry("ca-" + i++, cert);
        }
        tmf.init(ks);
        return tmf;
    }

    /** mTLS 클라이언트 키. 없으면 null */
    public static KeyManagerFactory keyManagers(String certPem, String keyPem) throws GeneralSecurityException {
        if (certPem == null || keyPem == null) {
            return null;
        }
        List<Certificate> chain = new ArrayList<>(certificates(certPem));
        KeyStore ks = emptyKeyStore();
        ks.setKeyEntry("client", privateKey(keyPem), NO_PASSWORD, chain.toArray(Certificate[]::new));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, NO_PASSWORD);
        return kmf;
    }

    /** 연결 테스트의 TLS 단계용 SSLContext */
    public static SSLContext sslContext(MqttSourceSettings s) throws GeneralSecurityException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf = keyManagers(s.clientCertPem(), s.clientKeyPem());
        ctx.init(kmf == null ? null : kmf.getKeyManagers(), trustManagers(s.caPem(), s.tlsInsecure()).getTrustManagers(),
                new SecureRandom());
        return ctx;
    }

    /** 인증서 체인 요약(연결 테스트 tlsChain): 주체와 만료일 */
    public static List<String> describe(Certificate[] chain) {
        List<String> lines = new ArrayList<>();
        if (chain == null) {
            return lines;
        }
        for (Certificate c : chain) {
            if (c instanceof X509Certificate x) {
                lines.add(x.getSubjectX500Principal().getName() + " (notAfter " + x.getNotAfter().toInstant() + ")");
            }
        }
        return lines;
    }

    static Collection<? extends Certificate> certificates(String pem) throws GeneralSecurityException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certs =
                cf.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
        if (certs.isEmpty()) {
            throw new GeneralSecurityException("PEM에 인증서가 없습니다");
        }
        return certs;
    }

    static PrivateKey privateKey(String pem) throws GeneralSecurityException {
        String base64 = pem.replaceAll("-----(BEGIN|END) [A-Z ]*PRIVATE KEY-----", "").replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(base64);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        for (String algorithm : new String[]{"RSA", "EC", "Ed25519"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException ignored) {
                // 다음 알고리즘
            }
        }
        throw new GeneralSecurityException("PKCS#8 개인 키를 읽을 수 없습니다(RSA·EC·Ed25519)");
    }

    private static KeyStore emptyKeyStore() throws GeneralSecurityException {
        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            return ks;
        } catch (java.io.IOException e) {
            throw new GeneralSecurityException(e);
        }
    }

    /** 개발 소스 전용(BR-DSC-29): 인증서를 검증하지 않는다 */
    private static final class InsecureTrustManagerFactory extends TrustManagerFactory {
        InsecureTrustManagerFactory() {
            super(new javax.net.ssl.TrustManagerFactorySpi() {
                @Override
                protected void engineInit(KeyStore ks) {
                }

                @Override
                protected void engineInit(javax.net.ssl.ManagerFactoryParameters spec) {
                }

                @Override
                protected TrustManager[] engineGetTrustManagers() {
                    return new TrustManager[]{new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }};
                }
            }, null, "insecure");
        }
    }
}
