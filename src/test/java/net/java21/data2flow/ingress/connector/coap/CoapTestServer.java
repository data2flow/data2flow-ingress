package net.java21.data2flow.ingress.connector.coap;

import net.java21.data2flow.contracts.test.connector.ContractPeer;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.config.CoapConfig;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.elements.config.UdpConfig;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 계약 IT의 상대 CoAP 서버(Eclipse Californium, 실제 UDP). 관찰 가능한 자원 {@code /kit}이 값마다 CON 알림 하나를 보낸다(ACK를 받아야
 * 다음 값). CoAP 관찰은 상대가 기록 확인을 받지 않으므로 확인 수가 없다(-1).
 */
final class CoapTestServer implements ContractPeer, AutoCloseable {

    static {
        CoapConfig.register();
        UdpConfig.register();
    }

    private final CoapServer server;
    private final KitResource resource = new KitResource();
    private final int port;

    CoapTestServer() {
        Configuration cfg = Configuration.createStandardWithoutFile();
        cfg.set(CoapConfig.NOTIFICATION_CHECK_INTERVAL_COUNT, 1_000_000);
        cfg.set(CoapConfig.NOTIFICATION_CHECK_INTERVAL_TIME, 1, java.util.concurrent.TimeUnit.DAYS);
        cfg.set(CoapConfig.PROTOCOL_STAGE_THREAD_COUNT, 1);   // 알림 순서(관찰 번호)가 뒤바뀌면 클라이언트가 옛것을 버린다
        CoapEndpoint endpoint = CoapEndpoint.builder().setConfiguration(cfg)
                .setInetSocketAddress(new InetSocketAddress("127.0.0.1", 0)).build();
        server = new CoapServer(cfg);
        server.addEndpoint(endpoint);
        server.add(resource);
        server.start();
        port = endpoint.getAddress().getPort();
    }

    String uri() {
        return "coap://127.0.0.1:" + port + "/kit";
    }

    @Override
    public void publish(List<byte[]> payloads) {
        for (byte[] p : payloads) {
            resource.push(p);
        }
    }

    @Override
    public long acknowledgedCount() {
        return -1;
    }

    @Override
    public void close() {
        resource.feeder.interrupt();
        server.destroy();
    }

    /**
     * 값이 바뀔 때마다 알림 하나. CoAP 관찰은 "상태 동기화"라 짧은 사이에 여러 번 바뀌면 서버가 알림을 합친다(마지막 값만 보냄, RFC 7641 §4.5).
     * 그래서 시험 상대는 실제 센서처럼 이전 값의 알림을 보낸 뒤에 다음 값으로 바꾼다(보내는 스레드 하나).
     */
    private static final class KitResource extends CoapResource {
        private final java.util.concurrent.BlockingQueue<byte[]> pending = new java.util.concurrent.LinkedBlockingQueue<>();
        private final Thread feeder;
        private byte[] current = new byte[0];
        private boolean served = true;

        KitResource() {
            super("kit");
            setObservable(true);
            setObserveType(CoAP.Type.CON);
            getAttributes().setObservable();
            feeder = Thread.ofVirtual().name("coap-kit-feeder").start(this::feed);
        }

        void push(byte[] value) {
            pending.add(value);
        }

        private void feed() {
            try {
                while (true) {
                    byte[] next = pending.take();
                    synchronized (this) {
                        current = next;
                        served = getObserverCount() == 0;
                                changed();
                        while (!served) {
                            wait(1000);
                            if (!served) {
                                changed();
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void handleGET(CoapExchange exchange) {
            byte[] value;
            synchronized (this) {
                value = current;
            }
            org.eclipse.californium.core.coap.Response response =
                    new org.eclipse.californium.core.coap.Response(CoAP.ResponseCode.CONTENT);
            response.setPayload(value);
            // 알림(CON)의 ACK를 받은 뒤에 다음 값으로 바꾼다(전송 중에 바꾸면 서버가 알림을 새 값으로 갈아 끼운다, RFC 7641 §4.5.2)
            response.addMessageObserver(new org.eclipse.californium.core.coap.MessageObserverAdapter() {
                @Override
                public void onAcknowledgement() {
                    synchronized (KitResource.this) {
                        served = true;
                        KitResource.this.notifyAll();
                    }
                }
            });
            exchange.respond(response);
        }
    }
}
