package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.contracts.connector.ConnectionErrorKind;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectionTestResult.Preview;
import net.java21.data2flow.contracts.connector.ConnectionTestResult.Step;
import net.java21.data2flow.ingress.connector.domain.ConnectionErrors;
import net.java21.data2flow.ingress.connector.mqtt.TlsSupport;

import javax.net.ssl.SSLContext;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 커넥터 공통 단계별 연결 테스트(DSC-09.11, BR-DSC-07, API-DSC-51). DNS → TCP → TLS → AUTH → SUBSCRIBE(또는 POLL) 순서로 해 보고
 * 실패한 단계에서 멈춘다(이후 단계는 SKIPPED). 결과는 저장하지 않고 예외를 던지지 않는다. 미리보기는 최대 10건이고 <b>확인(ack)하지
 * 않는다</b>(상대에 그대로 남아 운영 세션이 받는다).
 *
 * @param <H> 인증 단계가 만든 연결(구독 단계에 넘기고 끝나면 닫는다)
 */
public final class StagedConnectionTest<H extends AutoCloseable> {

    /** 인증(연결 맺기) */
    @FunctionalInterface
    public interface Auth<H> {
        H connect(Duration remaining) throws Exception;
    }

    /** 구독·첫 읽기. 미리보기를 돌려준다(확인하지 않는다) */
    @FunctionalInterface
    public interface Subscribe<H> {
        List<Preview> subscribe(H handle, Duration remaining) throws Exception;
    }

    private final String host;
    private final int port;
    private final boolean udp;
    private final SSLContext tls;
    private final boolean verifyHostname;
    private final Duration timeout;
    private final String subscribeStep;

    /**
     * @param host           접속 호스트
     * @param port           포트
     * @param udp            UDP 프로토콜(CoAP·BACnet)이면 TCP 단계를 건너뛴다
     * @param tls            TLS면 SSLContext, 아니면 null
     * @param verifyHostname 호스트 이름 검증(개발 소스의 검증 끄기면 false)
     * @param timeout        전체 제한 시간
     * @param subscribeStep  마지막 단계 이름(SUBSCRIBE 또는 POLL)
     */
    public StagedConnectionTest(String host, int port, boolean udp, SSLContext tls, boolean verifyHostname,
                                Duration timeout, String subscribeStep) {
        this.host = host;
        this.port = port;
        this.udp = udp;
        this.tls = tls;
        this.verifyHostname = verifyHostname;
        this.timeout = timeout;
        this.subscribeStep = subscribeStep;
    }

    public static String excerpt(byte[] payload) {
        return new String(payload, 0, Math.min(payload.length, Preview.MAX_EXCERPT_CHARS), StandardCharsets.UTF_8);
    }

    public static Preview preview(Clock clock, String topic, byte[] payload) {
        return new Preview(clock.instant(), topic, payload.length, excerpt(payload), null);
    }

    public ConnectionTestResult run(Auth<H> auth, Subscribe<H> subscribe, boolean lossPossible) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Step> steps = new ArrayList<>();
        long t0 = System.nanoTime();
        InetAddress address;
        try {
            address = resolve(host, remaining(deadline));
            steps.add(Step.ok(ConnectionTestResult.STEP_DNS, ms(t0)));
        } catch (Exception e) {
            steps.add(Step.failed(ConnectionTestResult.STEP_DNS, ms(t0), "DNS_NOT_FOUND", ConnectionErrors.describe(e)));
            return finish(steps, List.of(), ConnectionTestResult.STEP_TCP, lossPossible);
        }
        if (udp) {
            steps.add(Step.skipped(ConnectionTestResult.STEP_TCP));
            steps.add(Step.skipped(ConnectionTestResult.STEP_TLS));
        } else {
            t0 = System.nanoTime();
            boolean tcpOk = false;
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(address, port), (int) Math.max(1, remaining(deadline).toMillis()));
                tcpOk = true;
                steps.add(Step.ok(ConnectionTestResult.STEP_TCP, ms(t0)));
                if (tls != null) {
                    t0 = System.nanoTime();
                    try (SSLSocket ssl = (SSLSocket) tls.getSocketFactory().createSocket(socket, host, port, false)) {
                        ssl.setEnabledProtocols(TlsSupport.PROTOCOLS.stream()
                                .filter(p -> List.of(ssl.getSupportedProtocols()).contains(p)).toArray(String[]::new));
                        if (verifyHostname) {
                            var params = ssl.getSSLParameters();
                            params.setEndpointIdentificationAlgorithm("HTTPS");
                            ssl.setSSLParameters(params);
                        }
                        ssl.setSoTimeout((int) Math.max(1, remaining(deadline).toMillis()));
                        ssl.startHandshake();
                        steps.add(new Step(ConnectionTestResult.STEP_TLS, ConnectionTestResult.StepStatus.OK, ms(t0), null,
                                null, TlsSupport.describe(ssl.getSession().getPeerCertificates())));
                    }
                } else {
                    steps.add(Step.skipped(ConnectionTestResult.STEP_TLS));
                }
            } catch (Exception e) {
                if (!tcpOk) {
                    ConnectionErrorKind kind = ConnectionErrors.classify(e);
                    steps.add(Step.failed(ConnectionTestResult.STEP_TCP, ms(t0),
                            kind == ConnectionErrorKind.TIMEOUT ? "TCP_TIMEOUT" : "TCP_REFUSED", ConnectionErrors.describe(e)));
                    return finish(steps, List.of(), ConnectionTestResult.STEP_TLS, lossPossible);
                }
                String code = ConnectionErrors.describe(e).toLowerCase().contains("path") ? "TLS_CERT_CHAIN" : "TLS_HANDSHAKE";
                steps.add(Step.failed(ConnectionTestResult.STEP_TLS, ms(t0), code, ConnectionErrors.describe(e)));
                return finish(steps, List.of(), ConnectionTestResult.STEP_AUTH, lossPossible);
            }
        }
        t0 = System.nanoTime();
        H handle;
        try {
            handle = auth.connect(remaining(deadline));
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
            return finish(steps, List.of(), subscribeStep, lossPossible);
        }
        try (H h = handle) {
            t0 = System.nanoTime();
            try {
                List<Preview> preview = subscribe.subscribe(h, remaining(deadline));
                steps.add(Step.ok(subscribeStep, ms(t0)));
                return new ConnectionTestResult(steps, preview, lossPossible);
            } catch (Exception e) {
                steps.add(Step.failed(subscribeStep, ms(t0), subscribeStep + "_REJECTED", ConnectionErrors.describe(e)));
                return new ConnectionTestResult(steps, List.of(), lossPossible);
            }
        } catch (Exception closeFailure) {
            return new ConnectionTestResult(steps, List.of(), lossPossible);
        }
    }

    private ConnectionTestResult finish(List<Step> steps, List<Preview> preview, String skipFrom, boolean lossPossible) {
        boolean skipping = false;
        for (String name : List.of(ConnectionTestResult.STEP_TCP, ConnectionTestResult.STEP_TLS,
                ConnectionTestResult.STEP_AUTH, subscribeStep)) {
            skipping |= name.equals(skipFrom);
            if (skipping && steps.stream().noneMatch(s -> s.name().equals(name))) {
                steps.add(Step.skipped(name));
            }
        }
        return new ConnectionTestResult(steps, preview, lossPossible);
    }

    private static InetAddress resolve(String host, Duration timeout) throws Exception {
        CompletableFuture<InetAddress> f = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            try {
                f.complete(InetAddress.getByName(host));
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        try {
            return f.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }

    private static Duration remaining(long deadline) {
        return Duration.ofNanos(Math.max(1_000_000L, deadline - System.nanoTime()));
    }

    private static long ms(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
