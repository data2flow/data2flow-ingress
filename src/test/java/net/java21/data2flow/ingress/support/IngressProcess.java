package net.java21.data2flow.ingress.support;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;

/**
 * ingress를 <b>별도 JVM 프로세스</b>로 띄운다. {@link #kill9()}는 {@code Process.destroyForcibly()}(유닉스 SIGKILL = {@code kill -9})로
 * 정리 코드 없이 즉시 죽인다(NFR-02.09, TC-ING-017). {@link #terminate()}는 SIGTERM(graceful shutdown).
 *
 * <p>자식 프로세스 환경에서 {@code DATA2FLOW_*}·{@code MQTT_BASIC_AUTH}를 지워 개발자 PC의 .env 값(공용 인프라 주소·비밀번호)이 섞이지 않게 한다.
 */
public final class IngressProcess implements AutoCloseable {

    private final Process process;
    private final int managementPort;
    private final Path log;
    private final String name;

    private IngressProcess(String name, Process process, int managementPort, Path log) {
        this.name = name;
        this.process = process;
        this.managementPort = managementPort;
        this.log = log;
    }

    /**
     * @param instanceId 파드 이름 흉내(끝 번호가 client-id 번호)
     * @param properties 추가 설정(--key=value)
     */
    public static IngressProcess start(String instanceId, Map<String, String> properties) {
        try {
            int http = freePort();
            int mgmt = freePort();
            Path dir = Path.of("target", "ingress-processes");
            Files.createDirectories(dir);
            Path log = dir.resolve(instanceId + "-" + System.nanoTime() + ".log");
            List<String> cmd = new ArrayList<>();
            cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            cmd.add("-Xmx384m");
            cmd.add("-cp");
            cmd.add(System.getProperty("java.class.path"));
            cmd.add("net.java21.data2flow.ingress.IngressApplication");
            cmd.add("--server.port=" + http);
            cmd.add("--management.server.port=" + mgmt);
            cmd.add("--data2flow.ingress.instance-id=" + instanceId);
            cmd.add("--logging.level.net.java21.data2flow=INFO");
            properties.forEach((k, v) -> cmd.add("--" + k + "=" + v));
            ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile());
            pb.environment().keySet().removeIf(k -> k.startsWith("DATA2FLOW_") || k.equals("MQTT_BASIC_AUTH"));
            pb.environment().remove("HOSTNAME");
            return new IngressProcess(instanceId, pb.start(), mgmt, log);
        } catch (IOException e) {
            throw new IllegalStateException("ingress 프로세스를 시작할 수 없습니다", e);
        }
    }

    /** readiness UP이고 모든 소스가 CONNECTED가 될 때까지 기다린다 */
    public IngressProcess awaitConnected(Duration timeout) {
        HttpClient http = HttpClient.newHttpClient();
        await().atMost(timeout).pollInterval(Duration.ofMillis(300)).ignoreExceptions().until(() -> {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " 프로세스가 죽었습니다. 로그: " + log.toAbsolutePath());
            }
            HttpResponse<String> res = http.send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + managementPort + "/actuator/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            String body = res.body();
            return res.statusCode() == 200 && body.contains("\"sources\"") && body.contains("\"CONNECTED\"")
                    && !body.contains("\"CONNECTING\"") && !body.contains("\"DISCONNECTED\"");
        });
        return this;
    }

    /** kill -9: 정리 없이 즉시 종료 */
    public void kill9() {
        process.destroyForcibly();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** SIGTERM: graceful shutdown을 기다린다 */
    public int terminate(Duration timeout) {
        process.destroy();
        try {
            if (!process.waitFor(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
            }
            return process.exitValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }

    public boolean alive() {
        return process.isAlive();
    }

    public Path log() {
        return log;
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroyForcibly();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
