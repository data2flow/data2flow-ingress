package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectionTestResult.Preview;
import net.java21.data2flow.contracts.connector.ConnectionTestResult.Step;
import net.java21.data2flow.ingress.connector.domain.ConnectionErrors;

import javax.net.ssl.SSLSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 연결 테스트(DSC-02.05·09.11, BR-DSC-07, API-DSC-51). DNS → TCP → TLS → 인증(CONNECT) → 구독(SUBSCRIBE)을 차례로 해 보고,
 * 메시지를 최대 10건까지 받아 미리 보여 준다.
 *
 * <ul>
 *   <li>전체 제한 시간 안에서만 기다린다(기본 15초, 요청 최대 30초). 결과는 저장하지 않는다.</li>
 *   <li>client-id는 운영과 겹치지 않는 {@code {base}-test-{난수}}, clean start(세션을 남기지 않음)다.</li>
 *   <li><b>구독만 한다.</b> 발행 경로가 없다(CLAUDE.md §5).</li>
 *   <li>실패해도 예외를 던지지 않고 실패한 단계와 원인 코드를 결과에 담는다. 이후 단계는 SKIPPED.</li>
 * </ul>
 */
final class MqttConnectionTester {

    private final MqttSourceSettings settings;
    private final String clientId;
    private final Duration timeout;
    private final Clock clock;

    MqttConnectionTester(MqttSourceSettings settings, String clientId, Duration timeout, Clock clock) {
        this.settings = settings;
        this.clientId = clientId;
        this.timeout = timeout;
        this.clock = clock;
    }

    ConnectionTestResult run() {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Step> steps = new ArrayList<>();
        List<Preview> preview = new CopyOnWriteArrayList<>();

        // 1. DNS
        long t0 = System.nanoTime();
        InetAddress address;
        try {
            address = resolve(settings.host(), remaining(deadline));
            steps.add(Step.ok(ConnectionTestResult.STEP_DNS, ms(t0)));
        } catch (Exception e) {
            steps.add(Step.failed(ConnectionTestResult.STEP_DNS, ms(t0), "DNS_NOT_FOUND", ConnectionErrors.describe(e)));
            return finish(steps, preview, ConnectionTestResult.STEP_TCP);
        }
        // 2. TCP, 3. TLS
        t0 = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address, settings.port()), (int) Math.max(1, remaining(deadline)));
            steps.add(Step.ok(ConnectionTestResult.STEP_TCP, ms(t0)));
            if (settings.tls()) {
                t0 = System.nanoTime();
                try (SSLSocket ssl = (SSLSocket) TlsSupport.sslContext(settings).getSocketFactory()
                        .createSocket(socket, settings.host(), settings.port(), false)) {
                    ssl.setEnabledProtocols(TlsSupport.PROTOCOLS.stream()
                            .filter(p -> List.of(ssl.getSupportedProtocols()).contains(p)).toArray(String[]::new));
                    if (!settings.tlsInsecure()) {
                        var params = ssl.getSSLParameters();
                        params.setEndpointIdentificationAlgorithm("HTTPS");
                        ssl.setSSLParameters(params);
                    }
                    ssl.setSoTimeout((int) Math.max(1, remaining(deadline)));
                    ssl.startHandshake();
                    List<String> chain = TlsSupport.describe(ssl.getSession().getPeerCertificates());
                    steps.add(new Step(ConnectionTestResult.STEP_TLS, ConnectionTestResult.StepStatus.OK, ms(t0), null,
                            null, chain));
                }
            } else {
                steps.add(Step.skipped(ConnectionTestResult.STEP_TLS));
            }
        } catch (Exception e) {
            boolean tlsStage = steps.stream().anyMatch(s -> s.name().equals(ConnectionTestResult.STEP_TCP));
            if (!tlsStage) {
                ConnectionErrorKind kind = ConnectionErrors.classify(e);
                steps.add(Step.failed(ConnectionTestResult.STEP_TCP, ms(t0),
                        kind == ConnectionErrorKind.TIMEOUT ? "TCP_TIMEOUT" : "TCP_REFUSED", ConnectionErrors.describe(e)));
                return finish(steps, preview, ConnectionTestResult.STEP_TLS);
            }
            String code = ConnectionErrors.describe(e).toLowerCase().contains("path") ? "TLS_CERT_CHAIN" : "TLS_HANDSHAKE";
            steps.add(Step.failed(ConnectionTestResult.STEP_TLS, ms(t0), code, ConnectionErrors.describe(e)));
            return finish(steps, preview, ConnectionTestResult.STEP_AUTH);
        }

        // 4. 인증(CONNECT), 5. 구독
        CountDownLatch enough = new CountDownLatch(ConnectionTestResult.MAX_PREVIEW);
        ExecutorService executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("mqtt-test").factory());
        MqttLink link = new MqttLink(settings, clientId, Math.max(1, remaining(deadline)), in -> {
            in.ack().run();
            if (preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                String excerpt = new String(in.payload(), 0,
                        Math.min(in.payload().length, Preview.MAX_EXCERPT_CHARS), StandardCharsets.UTF_8);
                preview.add(new Preview(clock.instant(), in.topic(), in.payload().length, excerpt, null));
                enough.countDown();
            }
        }, cause -> { }, executor);
        try {
            t0 = System.nanoTime();
            try {
                await(link.connect(true, 0, 10), deadline);
                steps.add(Step.ok(ConnectionTestResult.STEP_AUTH, ms(t0)));
            } catch (Exception e) {
                ConnectionErrorKind kind = ConnectionErrors.classify(e);
                String code = switch (kind) {
                    case AUTH -> "AUTH_REJECTED";
                    case TIMEOUT -> "AUTH_TIMEOUT";
                    case TLS -> "TLS_HANDSHAKE";
                    default -> "CONNECT_FAILED";
                };
                steps.add(Step.failed(ConnectionTestResult.STEP_AUTH, ms(t0), code, ConnectionErrors.describe(e)));
                return finish(steps, preview, ConnectionTestResult.STEP_SUBSCRIBE);
            }
            t0 = System.nanoTime();
            try {
                await(link.subscribe(), deadline);
                steps.add(Step.ok(ConnectionTestResult.STEP_SUBSCRIBE, ms(t0)));
            } catch (Exception e) {
                steps.add(Step.failed(ConnectionTestResult.STEP_SUBSCRIBE, ms(t0), "SUBSCRIBE_REJECTED",
                        ConnectionErrors.describe(e)));
                return finish(steps, preview, null);
            }
            try {
                enough.await(remaining(deadline), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new ConnectionTestResult(steps, List.copyOf(preview), settings.lossPossible());
        } finally {
            link.disconnect().orTimeout(2, TimeUnit.SECONDS).exceptionally(e -> null).join();
            executor.shutdown();
        }
    }

    private ConnectionTestResult finish(List<Step> steps, List<Preview> preview, String skipFrom) {
        if (skipFrom != null) {
            boolean skipping = false;
            for (String name : List.of(ConnectionTestResult.STEP_TCP, ConnectionTestResult.STEP_TLS,
                    ConnectionTestResult.STEP_AUTH, ConnectionTestResult.STEP_SUBSCRIBE)) {
                skipping |= name.equals(skipFrom);
                if (skipping) {
                    steps.add(Step.skipped(name));
                }
            }
        }
        return new ConnectionTestResult(steps, List.copyOf(preview), settings.lossPossible());
    }

    private static InetAddress resolve(String host, long timeoutMillis) throws Exception {
        CompletableFuture<InetAddress> f = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            try {
                f.complete(InetAddress.getByName(host));
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        try {
            return f.get(Math.max(1, timeoutMillis), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }

    private static void await(CompletableFuture<?> f, long deadline) throws Exception {
        try {
            f.get(Math.max(1, remaining(deadline)), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            throw e.getCause() instanceof Exception ex ? ex : e;
        }
    }

    private static long remaining(long deadline) {
        return Math.max(0, (deadline - System.nanoTime()) / 1_000_000L);
    }

    private static long ms(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
