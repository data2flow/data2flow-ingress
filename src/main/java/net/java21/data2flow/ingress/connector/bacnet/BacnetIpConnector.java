package net.java21.data2flow.ingress.connector.bacnet;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * BACnet/IP 폴링 커넥터(키 {@code bacnet-ip}, ASHRAE 135, DSC-09 카탈로그 "산업·빌딩"). <b>BACnet4J는 GPL-3.0이라 쓸 수 없어(ADR-018)</b>
 * ReadProperty·ReadRange만 직접 구현했다({@link BacnetCodec}). 리더 1대만 폴링한다(SINGLETON). 쓰기·COV 구독은 하지 않는다.
 *
 * <ul>
 *   <li><b>present-value(기본):</b> 주기마다 객체의 Present_Value를 읽어 {@code {"device":1234,"values":{"zoneTemp":22.5}}} 하나를 넘긴다
 *       (같은 값도 주기마다 저장되도록 수신 시각 버킷 중복 키). 위치는 마지막 폴링 시각.</li>
 *   <li><b>trend-log:</b> 추세 기록(Trend Log) 객체의 Log_Buffer를 ReadRange(bySequenceNumber)로 저장된 번호 다음부터 읽어 기록 하나를
 *       {@code {"device":1234,"log":1,"seq":7,"time":"…","value":22.5}} 하나로 넘기고, 모두 기록(confirm)한 뒤에 번호를 저장한다(무손실, CURSOR).</li>
 * </ul>
 */
public class BacnetIpConnector implements SourceConnector {

    public static final String KEY = "bacnet-ip";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** 객체 유형 이름 → 번호(§21 BACnetObjectType) */
    static final Map<String, Integer> OBJECT_TYPES = Map.ofEntries(
            Map.entry("analog-input", 0), Map.entry("analog-output", 1), Map.entry("analog-value", 2),
            Map.entry("binary-input", 3), Map.entry("binary-output", 4), Map.entry("binary-value", 5),
            Map.entry("device", 8), Map.entry("multi-state-input", 13), Map.entry("multi-state-output", 14),
            Map.entry("multi-state-value", 19), Map.entry("trend-log", 20));

    private final PollingOptions options;
    private final Clock clock;

    public BacnetIpConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "BACnet/IP", "1.0.0", ConnectorCategory.INDUSTRIAL, Set.of(AuthMethod.NONE),
                Set.of(PayloadFormat.JSON), AckMode.CURSOR, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config, options);
        return new StagedConnectionTest<Client>(s.host, s.port, true, null, true, options.testTimeout(), "POLL")
                .run(remaining -> {
                    Client c = new Client(s);
                    c.readProperty(BacnetCodec.OBJECT_DEVICE, s.deviceInstance, BacnetCodec.PROP_OBJECT_NAME);
                    return c;
                }, (c, remaining) -> {
                    byte[] payload = s.trendLog ? JSON.writeValueAsBytes(JSON.createObjectNode().put("device", s.deviceInstance)
                            .put("log", s.logInstance)) : snapshot(c, s);
                    return List.of(StagedConnectionTest.preview(clock, s.topic, payload));
                }, false);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config, options), sink, ctx);
    }

    /** 측정점 하나 */
    record Point(String name, int type, int instance) {
    }

    record Settings(String host, int port, int deviceInstance, boolean trendLog, int logInstance, int batch,
                    List<Point> points, Duration interval, String topic) {
        static Settings from(SourceConfig config, PollingOptions options) {
            Cfg c = Cfg.of(config);
            String mode = c.text("mode", "present-value").toLowerCase(Locale.ROOT);
            if (!Set.of("present-value", "trend-log").contains(mode)) {
                throw InvalidSettingsException.config("mode");
            }
            List<Point> points = new ArrayList<>();
            for (JsonNode p : c.node().path("points")) {
                Integer type = OBJECT_TYPES.get(p.path("objectType").asString("").toLowerCase(Locale.ROOT));
                int inst = p.path("instance").asInt(-1);
                String name = p.path("name").asString("");
                if (type == null || inst < 0 || inst > 0x3FFFFE || !name.matches("[A-Za-z0-9_.-]{1,64}")) {
                    throw InvalidSettingsException.config("points");
                }
                points.add(new Point(name, type, inst));
            }
            boolean trend = mode.equals("trend-log");
            if (!trend && points.isEmpty() || points.size() > 200) {
                throw InvalidSettingsException.config("points");
            }
            int device = c.integer("deviceInstance", 0, 0, 0x3FFFFE);
            return new Settings(c.required("host"), c.integer("port", BacnetCodec.PORT, 1, 65535), device, trend,
                    c.integer("trendLogInstance", 1, 0, 0x3FFFFE), c.integer("batchSize", 50, 1, 200), List.copyOf(points),
                    options.pollInterval(c.seconds("intervalSec", 60, 1, 86_400)), c.text("topic", "bacnet/" + device));
        }
    }

    static byte[] snapshot(Client c, Settings s) throws IOException {
        ObjectNode o = JSON.createObjectNode().put("device", s.deviceInstance);
        ObjectNode values = o.putObject("values");
        for (Point p : s.points) {
            values.set(p.name(), JSON.valueToTree(c.readProperty(p.type(), p.instance(), BacnetCodec.PROP_PRESENT_VALUE)));
        }
        return JSON.writeValueAsBytes(o);
    }

    /** UDP 요청·응답(APDU 제한 3초, 3번 재시도, §5.4.4) */
    static final class Client implements AutoCloseable {
        private final DatagramSocket socket;
        private final InetSocketAddress target;
        private int invokeId;

        Client(Settings s) throws IOException {
            this.socket = new DatagramSocket(0);
            this.socket.setSoTimeout(3000);
            this.target = new InetSocketAddress(InetAddress.getByName(s.host), s.port);
        }

        Object readProperty(int type, int instance, int property) throws IOException {
            return BacnetCodec.readPropertyValue(request(id -> BacnetCodec.readProperty(id, type, instance, property)));
        }

        List<BacnetCodec.LogRecord> readRange(int instance, long from, int count) throws IOException {
            return BacnetCodec.readRangeRecords(request(id ->
                    BacnetCodec.readRangeBySequence(id, BacnetCodec.OBJECT_TREND_LOG, instance, from, count)));
        }

        private synchronized byte[] request(java.util.function.IntFunction<byte[]> apdu) throws IOException {
            int id = invokeId = (invokeId + 1) & 0xFF;
            byte[] frame = BacnetCodec.frame(apdu.apply(id));
            byte[] buf = new byte[1500];
            for (int attempt = 0; attempt < 3; attempt++) {
                socket.send(new DatagramPacket(frame, frame.length, target));
                long deadline = System.nanoTime() + 3_000_000_000L;
                while (System.nanoTime() < deadline) {
                    DatagramPacket p = new DatagramPacket(buf, buf.length);
                    try {
                        socket.receive(p);
                    } catch (SocketTimeoutException e) {
                        break;
                    }
                    byte[] reply = BacnetCodec.apdu(p.getData(), p.getLength());
                    if (reply.length > 2 && BacnetCodec.invokeId(reply) == id) {
                        return reply;
                    }
                }
            }
            throw new SocketTimeoutException("BACnet 응답이 없습니다(3초 × 3회)");
        }

        @Override
        public void close() {
            socket.close();
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Client client;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), settings.interval, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            client = new Client(settings);
            client.readProperty(BacnetCodec.OBJECT_DEVICE, settings.deviceInstance, BacnetCodec.PROP_OBJECT_NAME);
        }

        @Override
        protected boolean pollOnce() throws Exception {
            if (!settings.trendLog) {
                byte[] payload = snapshot(client, settings);
                Instant now = ctx.clock().instant();
                Duration bucket = settings.interval.dividedBy(2);
                writeAll(List.of(RawEnvelope.of(config.organizationId(), config.sourceId(), config.sourceType(), settings.topic,
                        payload, now, ctx.instanceId(), DedupKeys.contentInBucket(config.sourceId(), settings.topic, payload,
                                now, bucket.isZero() ? Duration.ofSeconds(1) : bucket))));
                ctx.cursorStore().save(config.sourceId(), PollCursor.at(now.toString()));
                return false;
            }
            long last = ctx.cursorStore().load(config.sourceId()).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
            List<BacnetCodec.LogRecord> records = client.readRange(settings.logInstance, last + 1, settings.batch);
            List<RawEnvelope> batch = new ArrayList<>();
            long newest = last;
            for (BacnetCodec.LogRecord r : records) {
                if (r.sequence() <= last) {
                    continue;
                }
                ObjectNode o = JSON.createObjectNode().put("device", settings.deviceInstance).put("log", settings.logInstance)
                        .put("seq", r.sequence()).put("time", r.timestamp().toInstant(ZoneOffset.UTC).toString());
                o.set("value", JSON.valueToTree(r.value()));
                batch.add(envelope(settings.topic, JSON.writeValueAsBytes(o)));
                newest = Math.max(newest, r.sequence());
            }
            if (batch.isEmpty() || isPaused()) {
                return false;
            }
            writeAll(batch);   // 실패하면 번호를 저장하지 않는다
            ctx.cursorStore().save(config.sourceId(), PollCursor.at(Long.toString(newest)));
            return true;
        }

        @Override
        protected void disconnect() {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
