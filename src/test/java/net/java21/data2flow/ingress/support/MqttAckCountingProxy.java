package net.java21.data2flow.ingress.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 테스트용 TCP 프록시: 구독 클라이언트(ingress) ↔ 테스트 브로커 사이에서 바이트를 그대로 넘기면서, 클라이언트가 보낸 MQTT 확인
 * 패킷(PUBACK·PUBREC)을 센다. 커넥터 계약 키트의 "기록 전 확인 금지"(AT-DSC-18.1)를 브로커 쪽에서 실제 패킷으로 확인한다.
 *
 * <p>WebSocket 모드는 HTTP 업그레이드 뒤의 프레임을 풀어(클라이언트 프레임 마스크 해제) MQTT 패킷을 센다. {@link #cut()}은 모든 연결을
 * 즉시 끊는다(DISCONNECT 없이 소켓을 닫아 프로세스가 죽거나 네트워크가 끊긴 것처럼 보이게 한다). {@link #block(boolean)}은 새 연결을 거부한다.
 */
public final class MqttAckCountingProxy implements AutoCloseable {

    private final ServerSocket server;
    private final String targetHost;
    private final int targetPort;
    private final boolean webSocket;
    private final AtomicLong acks = new AtomicLong();
    private final AtomicLong connections = new AtomicLong();
    private final List<Socket> open = new CopyOnWriteArrayList<>();
    private final List<Long> acceptedAtNanos = new CopyOnWriteArrayList<>();
    private volatile boolean blocked;
    private volatile boolean closed;

    public MqttAckCountingProxy(String targetHost, int targetPort, boolean webSocket) throws IOException {
        this.server = new ServerSocket();
        this.server.bind(new InetSocketAddress("0.0.0.0", 0));
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.webSocket = webSocket;
        Thread.ofPlatform().daemon().name("mqtt-proxy-accept").start(this::acceptLoop);
    }

    public int port() {
        return server.getLocalPort();
    }

    /** 클라이언트가 보낸 PUBACK·PUBREC 누적 수 */
    public long acknowledgedCount() {
        return acks.get();
    }

    /** 지금까지 받아들인 연결 수(재연결 확인) */
    public long connectionCount() {
        return connections.get();
    }

    /** 접속 시도 시각(System.nanoTime, 막힌 동안의 시도 포함). 재연결 간격 확인(TC-DSC-069) */
    public List<Long> acceptedAtNanos() {
        return List.copyOf(acceptedAtNanos);
    }

    /** 모든 연결을 즉시 끊는다(DISCONNECT 없음) */
    public void cut() {
        for (Socket s : open) {
            closeQuietly(s);
        }
        open.clear();
    }

    /** true면 새 연결을 받자마자 끊는다(브로커 장애 흉내) */
    public void block(boolean value) {
        blocked = value;
        if (value) {
            cut();
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                acceptedAtNanos.add(System.nanoTime());
                if (blocked) {
                    closeQuietly(client);
                    continue;
                }
                Socket upstream = new Socket(targetHost, targetPort);
                client.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
                open.add(client);
                open.add(upstream);
                connections.incrementAndGet();
                Thread.ofPlatform().daemon().start(() -> pump(client, upstream, true));
                Thread.ofPlatform().daemon().start(() -> pump(upstream, client, false));
            } catch (IOException e) {
                if (closed) {
                    return;
                }
            }
        }
    }

    private void pump(Socket from, Socket to, boolean clientToBroker) {
        MqttCounter counter = clientToBroker ? new MqttCounter(webSocket) : null;
        byte[] buf = new byte[16 * 1024];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (counter != null) {
                    counter.feed(buf, n);
                }
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException ignored) {
            // 끊김
        } finally {
            closeQuietly(from);
            closeQuietly(to);
            open.remove(from);
            open.remove(to);
        }
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(server);
        cut();
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // 무시
        }
    }

    /** 바이트 흐름에서 MQTT 패킷 경계를 따라가며 확인 패킷을 센다 */
    private final class MqttCounter {
        private final boolean ws;
        // HTTP 업그레이드 요청 끝(\r\n\r\n)을 찾는 중
        private boolean httpDone;
        private int crlf;
        // WebSocket 프레임 상태
        private final byte[] frameHeader = new byte[14];
        private int frameHeaderLen;
        private long framePayloadLeft;
        private final byte[] mask = new byte[4];
        private boolean masked;
        private long framePos;
        private boolean frameIsData;
        // MQTT 상태
        private int mqttState; // 0: 고정 헤더 첫 바이트, 1: 남은 길이, 2: 본문
        private int multiplier = 1;
        private long remaining;
        private int lengthBytes;

        MqttCounter(boolean ws) {
            this.ws = ws;
        }

        void feed(byte[] b, int n) {
            for (int i = 0; i < n; i++) {
                if (!ws) {
                    mqtt(b[i]);
                } else {
                    websocket(b[i]);
                }
            }
        }

        private void websocket(byte value) {
            if (!httpDone) {
                int expected = (crlf % 2 == 0) ? '\r' : '\n';
                crlf = value == expected ? crlf + 1 : (value == '\r' ? 1 : 0);
                httpDone = crlf == 4;
                return;
            }
            if (framePayloadLeft == 0 && frameHeaderLen >= 0) {
                frameHeader[frameHeaderLen++] = value;
                if (frameHeaderLen < 2) {
                    return;
                }
                int len7 = frameHeader[1] & 0x7F;
                boolean m = (frameHeader[1] & 0x80) != 0;
                int need = 2 + (len7 == 126 ? 2 : len7 == 127 ? 8 : 0) + (m ? 4 : 0);
                if (frameHeaderLen < need) {
                    return;
                }
                long len = len7;
                int pos = 2;
                if (len7 == 126) {
                    len = ((frameHeader[2] & 0xFF) << 8) | (frameHeader[3] & 0xFF);
                    pos = 4;
                } else if (len7 == 127) {
                    len = 0;
                    for (int k = 0; k < 8; k++) {
                        len = (len << 8) | (frameHeader[2 + k] & 0xFF);
                    }
                    pos = 10;
                }
                masked = m;
                if (m) {
                    System.arraycopy(frameHeader, pos, mask, 0, 4);
                }
                int opcode = frameHeader[0] & 0x0F;
                frameIsData = opcode == 0 || opcode == 2;
                framePayloadLeft = len;
                framePos = 0;
                frameHeaderLen = 0;
                return;
            }
            byte v = masked ? (byte) (value ^ mask[(int) (framePos % 4)]) : value;
            framePos++;
            framePayloadLeft--;
            if (frameIsData) {
                mqtt(v);
            }
        }

        private void mqtt(byte value) {
            switch (mqttState) {
                case 0 -> {
                    int type = (value & 0xF0) >> 4;
                    if (type == 4 || type == 5) {
                        acks.incrementAndGet();
                    }
                    mqttState = 1;
                    multiplier = 1;
                    remaining = 0;
                    lengthBytes = 0;
                }
                case 1 -> {
                    remaining += (long) (value & 0x7F) * multiplier;
                    multiplier *= 128;
                    lengthBytes++;
                    if ((value & 0x80) == 0 || lengthBytes == 4) {
                        mqttState = remaining == 0 ? 0 : 2;
                    }
                }
                default -> {
                    remaining--;
                    if (remaining == 0) {
                        mqttState = 0;
                    }
                }
            }
        }
    }
}
