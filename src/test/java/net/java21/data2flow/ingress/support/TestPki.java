package net.java21.data2flow.ingress.support;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 테스트 실행 때마다 만드는 임시 인증서(CA·서버·클라이언트). JDK keytool로 만들고 PEM으로 내보낸다. 저장소에 키를 두지 않는다.
 * 서버 인증서 SAN은 localhost·127.0.0.1(테스트 컨테이너 nginx가 이 이름으로 응답한다).
 */
public final class TestPki {

    private static final String PASS = "changeit";
    private final Path dir;
    private final String caPem;
    private final String serverCertPem;
    private final String serverKeyPem;
    private final String clientCertPem;
    private final String clientKeyPem;

    private TestPki(Path dir) throws Exception {
        this.dir = dir;
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048", "-dname", "CN=data2flow-test-ca",
                "-ext", "bc:c", "-validity", "2", "-keystore", ks("ca"));
        caPem = exportCert("ca", "ca");
        serverCertPem = issue("server", "CN=localhost", "san=dns:localhost,ip:127.0.0.1", "eku=serverAuth");
        serverKeyPem = exportKey("server");
        clientCertPem = issue("client", "CN=data2flow-ingress-test", null, "eku=clientAuth");
        clientKeyPem = exportKey("client");
    }

    public static TestPki create() {
        try {
            return new TestPki(Files.createTempDirectory("d2f-pki"));
        } catch (Exception e) {
            throw new IllegalStateException("테스트 인증서를 만들 수 없습니다", e);
        }
    }

    public String caPem() {
        return caPem;
    }

    public String serverCertPem() {
        return serverCertPem;
    }

    public String serverKeyPem() {
        return serverKeyPem;
    }

    public String clientCertPem() {
        return clientCertPem;
    }

    public String clientKeyPem() {
        return clientKeyPem;
    }

    /** nginx에 넣을 서버 인증서 체인(서버 + CA) */
    public String serverChainPem() {
        return serverCertPem + caPem;
    }

    private String issue(String alias, String dname, String san, String eku) throws Exception {
        keytool("-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-dname", dname,
                "-validity", "2", "-keystore", ks(alias));
        Path csr = dir.resolve(alias + ".csr");
        keytool("-certreq", "-alias", alias, "-keystore", ks(alias), "-file", csr.toString());
        Path crt = dir.resolve(alias + ".crt");
        List<String> args = new ArrayList<>(List.of("-gencert", "-alias", "ca", "-keystore", ks("ca"), "-infile",
                csr.toString(), "-outfile", crt.toString(), "-rfc", "-validity", "2", "-ext", eku));
        if (san != null) {
            args.add("-ext");
            args.add(san);
        }
        keytool(args.toArray(String[]::new));
        return Files.readString(crt);
    }

    private String exportCert(String alias, String store) throws Exception {
        Path out = dir.resolve(alias + ".pem");
        keytool("-exportcert", "-alias", alias, "-keystore", ks(store), "-rfc", "-file", out.toString());
        return Files.readString(out);
    }

    private String exportKey(String alias) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(Path.of(ks(alias)))) {
            ks.load(in, PASS.toCharArray());
        }
        Key key = ks.getKey(alias, PASS.toCharArray());
        String b64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(key.getEncoded());
        return "-----BEGIN PRIVATE KEY-----\n" + b64 + "\n-----END PRIVATE KEY-----\n";
    }

    private String ks(String name) {
        return dir.resolve(name + ".p12").toString();
    }

    private void keytool(String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(List.of(args));
        cmd.addAll(List.of("-storetype", "PKCS12", "-storepass", PASS, "-keypass", PASS, "-noprompt"));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IOException("keytool 실패: " + String.join(" ", args) + "\n" + output);
        }
    }
}
