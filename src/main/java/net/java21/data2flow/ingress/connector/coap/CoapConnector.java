package net.java21.data2flow.ingress.connector.coap;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.common.WriteFailedException;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapObserveRelation;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.config.CoapConfig;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.elements.config.UdpConfig;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * CoAP observe 커넥터(키 {@code coap}, RFC 7252·7641, connectors.md §2 Eclipse Californium EPL-2.0/EDL-1.0 조건부). 자원 하나 이상을
 * 관찰(observe)하고 알림 payload 하나를 원본 하나로 넘긴다(주제 = 자원 경로).
 *
 * <p>CoAP 알림은 상대가 다시 보내 주지 않으므로 확인 방식은 NONE(화면 "유실 가능")이다. CON 알림의 ACK는 Californium이 받자마자 보낸다.
 * 일시정지 동안 받은 알림은 메모리 대기열(최대 10,000건)에 두었다가 재개하면 넘긴다. 관찰 관계는 세션 하나라 리더 1대만 실행한다
 * (SINGLETON, BR-DSC-25). DTLS(Scandium)는 아직 넣지 않았다(평문 {@code coap://}만).
 */
public class CoapConnector implements SourceConnector {

    public static final String KEY = "coap";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    static {
        CoapConfig.register();
        UdpConfig.register();
    }

    private final PollingOptions options;
    private final Clock clock;

    public CoapConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "CoAP observe", "1.0.0", ConnectorCategory.LIGHTWEIGHT, Set.of(AuthMethod.NONE),
                Set.of(PayloadFormat.JSON, PayloadFormat.CBOR, PayloadFormat.TEXT, PayloadFormat.BINARY),
                AckMode.NONE, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    static CoapEndpoint endpoint() {
        Configuration cfg = Configuration.createStandardWithoutFile();
        cfg.set(UdpConfig.UDP_RECEIVE_BUFFER_SIZE, 4 * 1024 * 1024);   // 알림이 몰려도 소켓에서 버려지지 않게
        // 알림을 한 스레드가 받은 순서대로 처리한다. 여러 스레드면 순서가 뒤바뀌어 관찰 번호 검사(RFC 7641 §3.4)가 앞선 알림을 버린다
        cfg.set(CoapConfig.PROTOCOL_STAGE_THREAD_COUNT, 1);
        return CoapEndpoint.builder().setConfiguration(cfg).build();
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        URI first = s.resources.get(0);
        return new StagedConnectionTest<Client>(first.getHost(), first.getPort() > 0 ? first.getPort() : 5683, true, null,
                true, options.testTimeout(), ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> new Client(endpoint()), (c, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    for (URI r : s.resources) {
                        CoapClient client = c.client(r, remaining);
                        CoapResponse res = client.get();
                        if (res == null || !res.isSuccess()) {
                            throw new IOException("GET " + r.getPath() + " 실패: " + (res == null ? "응답 없음" : res.getCode()));
                        }
                        if (preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                            preview.add(StagedConnectionTest.preview(clock, r.getPath(), res.getPayload()));
                        }
                    }
                    return preview;
                }, true);
    }

    /** 끝점 하나를 함께 쓰는 클라이언트들 */
    private static final class Client implements AutoCloseable {
        private final CoapEndpoint endpoint;
        private final List<CoapClient> clients = new ArrayList<>();

        Client(CoapEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        CoapClient client(URI uri, Duration timeout) {
            CoapClient c = new CoapClient(uri);
            c.setEndpoint(endpoint);
            c.setTimeout(Math.max(1000, timeout.toMillis()));
            clients.add(c);
            return c;
        }

        @Override
        public void close() {
            clients.forEach(CoapClient::shutdown);
            endpoint.destroy();
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    record Settings(List<URI> resources, boolean confirmable) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            List<URI> resources = new ArrayList<>();
            for (String r : c.strings("resources")) {
                URI u = URI.create(r);
                if (!"coap".equals(u.getScheme()) || u.getHost() == null) {
                    throw InvalidSettingsException.config("resources");
                }
                resources.add(u);
            }
            if (resources.isEmpty() || resources.size() > 50) {
                throw InvalidSettingsException.config("resources");
            }
            return new Settings(List.copyOf(resources), c.bool("confirmable", true));
        }
    }

    private record Notification(String topic, byte[] payload) {
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private final BlockingDeque<Notification> queue = new LinkedBlockingDeque<>(10_000);
        private Client client;
        private final List<CoapObserveRelation> relations = new ArrayList<>();
        private volatile String failure;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            failure = null;
            client = new Client(endpoint());
            for (URI r : settings.resources) {
                CoapClient c = client.client(r, options.connectTimeout());
                if (settings.confirmable) {
                    c.useCONs();
                } else {
                    c.useNONs();
                }
                String topic = r.getPath();
                CoapObserveRelation rel = c.observeAndWait(new CoapHandler() {
                    @Override
                    public void onLoad(CoapResponse response) {
                        if (response.isSuccess() && response.getPayloadSize() > 0 && !queue.offer(
                                new Notification(topic, response.getPayload()))) {
                            failure = "대기열이 가득 찼습니다";
                        }
                    }

                    @Override
                    public void onError() {
                        failure = "관찰 오류: " + topic;
                    }
                });
                if (rel == null || rel.isCanceled() || rel.getCurrent() == null) {
                    throw new IOException("관찰 등록 실패: " + topic);
                }
                relations.add(rel);
            }
        }

        @Override
        protected boolean pollOnce() throws Exception {
            if (failure != null) {
                throw new IOException(failure);
            }
            Notification first = queue.poll(Math.max(50, options.idleInterval().toMillis()), TimeUnit.MILLISECONDS);
            if (first == null) {
                return false;
            }
            List<Notification> list = new ArrayList<>();
            list.add(first);
            queue.drainTo(list, 499);
            if (isPaused()) {   // 일시정지 중이면 넘기지 않고 대기열 앞에 되돌린다
                for (int i = list.size() - 1; i >= 0; i--) {
                    queue.offerFirst(list.get(i));
                }
                return false;
            }
            List<RawEnvelope> batch = new ArrayList<>(list.size());
            for (Notification n : list) {
                batch.add(envelope(n.topic(), n.payload()));
            }
            try {
                writeAll(batch);
            } catch (WriteFailedException e) {
                throw new IOException("기록 실패: 알림은 다시 오지 않는다(유실 가능, AckMode NONE)", e);
            }
            return true;
        }

        @Override
        protected void disconnect() {
            relations.forEach(CoapObserveRelation::proactiveCancel);
            relations.clear();
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
