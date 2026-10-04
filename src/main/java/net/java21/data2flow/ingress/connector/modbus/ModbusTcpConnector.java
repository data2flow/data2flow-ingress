package net.java21.data2flow.ingress.connector.modbus;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;
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
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Modbus TCP 폴링 커넥터(키 {@code modbus-tcp}, 소스 유형 MODBUS_TCP, DSC-05.01, connectors.md §2 j2mod Apache-2.0). 읽기 함수
 * (01·02·03·04)만 쓰고 쓰기 함수는 쓰지 않는다(송신은 DSC-09.13, M7). 리더 1대만 폴링한다(SINGLETON).
 *
 * <ul>
 *   <li><b>snapshot(기본):</b> 주기(10초 이상)마다 측정점을 읽어 {@code {"unitId":1,"values":{"temp":22.5}}} 하나를 넘긴다. 같은 값이 주기마다
 *       와도 저장되도록 중복 키는 수신 시각 버킷(주기의 절반)을 섞는다(BR-ING-07). 위치는 마지막으로 기록한 폴링 시각(CURSOR).</li>
 *   <li><b>log:</b> 기록 카운터(UINT32)와 고리형 기록 영역을 가진 장비(전력량계 부하 기록 등)에서, 저장된 위치(기록 번호) 다음 것부터
 *       {@code {"unitId":1,"registers":[…]}}를 하나씩 넘기고 모두 기록(confirm)한 뒤에 위치를 저장한다(무손실, BR-DSC-24). 끊긴 동안 고리 크기보다
 *       많이 쌓이면 덮어쓴 만큼은 잃는다(로그에 남김).</li>
 * </ul>
 */
public class ModbusTcpConnector implements SourceConnector {

    public static final String KEY = "modbus-tcp";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** 한 요청에 읽을 수 있는 레지스터 수(Modbus 사양) */
    static final int MAX_REGISTERS = 125;

    private final PollingOptions options;
    private final Clock clock;

    public ModbusTcpConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "Modbus TCP", "1.0.0", ConnectorCategory.INDUSTRIAL, Set.of(AuthMethod.NONE),
                Set.of(PayloadFormat.JSON), AckMode.CURSOR, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config, options);
        return new StagedConnectionTest<Master>(s.host, s.port, false, null, true, options.testTimeout(), "POLL")
                .run(remaining -> new Master(connect(s, remaining)), (m, remaining) -> {
                    byte[] payload = s.log ? JSON.writeValueAsBytes(JSON.createObjectNode().put("unitId", s.unitId)
                            .put("records", readCounter(m.master, s))) : snapshot(m.master, s);
                    return List.of(StagedConnectionTest.preview(clock, s.topic, payload));
                }, false);
    }

    private record Master(ModbusTCPMaster master) implements AutoCloseable {
        @Override
        public void close() {
            master.disconnect();
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config, options), sink, ctx);
    }

    static ModbusTCPMaster connect(Settings s, Duration timeout) throws Exception {
        ModbusTCPMaster m = new ModbusTCPMaster(s.host, s.port, (int) Math.max(1000, timeout.toMillis()), false);
        m.connect();
        return m;
    }

    /** 측정점을 모두 읽어 payload 하나로 */
    static byte[] snapshot(ModbusTCPMaster m, Settings s) throws Exception {
        ObjectNode o = JSON.createObjectNode().put("unitId", s.unitId);
        ObjectNode values = o.putObject("values");
        for (ModbusPoint p : s.points) {
            switch (p.function()) {
                case "COIL" -> values.put(p.name(), m.readCoils(s.unitId, p.address(), 1).getBit(0));
                case "DISCRETE" -> values.put(p.name(), m.readInputDiscretes(s.unitId, p.address(), 1).getBit(0));
                default -> {
                    int[] w = words(p.function().equals("INPUT")
                            ? m.readInputRegisters(s.unitId, p.address(), p.width())
                            : m.readMultipleRegisters(s.unitId, p.address(), p.width()));
                    Number n = p.decode(w);
                    if (n instanceof Long l) {
                        values.put(p.name(), l);
                    } else {
                        values.put(p.name(), n.doubleValue());
                    }
                }
            }
        }
        return JSON.writeValueAsBytes(o);
    }

    static int[] words(InputRegister[] regs) {
        int[] w = new int[regs.length];
        for (int i = 0; i < regs.length; i++) {
            w[i] = regs[i].toUnsignedShort();
        }
        return w;
    }

    static long readCounter(ModbusTCPMaster m, Settings s) throws Exception {
        int[] w = words(m.readMultipleRegisters(s.unitId, s.counterAddress, 2));
        return ((long) w[0] << 16) | w[1];
    }

    /** 설정({@code modbus-tcp.schema.json}) */
    record Settings(String host, int port, int unitId, String mode, Duration interval, List<ModbusPoint> points,
                    int counterAddress, int recordAddress, int recordLength, int ringSize, boolean log, String topic) {
        static Settings from(SourceConfig config, PollingOptions options) {
            Cfg c = Cfg.of(config);
            String host = c.required("host");
            int port = c.integer("port", 502, 1, 65535);
            int unit = c.integer("unitId", 1, 0, 255);
            String mode = c.text("mode", "snapshot").toLowerCase(Locale.ROOT);
            Duration interval = options.pollInterval(c.seconds("intervalSec", 60, 1, 86_400));
            List<ModbusPoint> points = new ArrayList<>();
            for (JsonNode p : c.node().path("points")) {
                String name = p.path("name").asString("");
                if (!name.matches("[A-Za-z0-9_.-]{1,64}")) {
                    throw InvalidSettingsException.config("points");
                }
                points.add(ModbusPoint.of(name, p.has("register") ? p.path("register").asString() : null,
                        p.path("function").asString(null), p.has("address") ? p.path("address").asInt() : null,
                        p.path("type").asString(null), p.path("wordOrder").asString("BIG"), p.path("scale").asDouble(1),
                        p.path("offset").asDouble(0)));
            }
            boolean log = mode.equals("log");
            if (!log && !mode.equals("snapshot") || !log && points.isEmpty() || points.size() > 200) {
                throw InvalidSettingsException.config(log ? "mode" : "points");
            }
            Cfg l = c.child("log");
            int len = l.integer("recordLength", 4, 1, MAX_REGISTERS);
            int ring = l.integer("ringSize", 1000, 1, 60_000);
            return new Settings(host, port, unit, mode, interval, List.copyOf(points), l.integer("counterAddress", 0, 0, 65534),
                    l.integer("recordAddress", 100, 0, 65535), len, ring, log, c.text("topic", "modbus/" + unit));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private ModbusTCPMaster master;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), settings.interval, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            master = ModbusTcpConnector.connect(settings, options.connectTimeout());
        }

        @Override
        protected boolean pollOnce() throws Exception {
            return settings.log ? pollLog() : pollSnapshot();
        }

        private boolean pollSnapshot() throws Exception {
            byte[] payload = snapshot(master, settings);
            Instant now = ctx.clock().instant();
            Duration bucket = settings.interval.dividedBy(2);
            RawEnvelope e = RawEnvelope.of(config.organizationId(), config.sourceId(), config.sourceType(), settings.topic,
                    payload, now, ctx.instanceId(),
                    DedupKeys.contentInBucket(config.sourceId(), settings.topic, payload, now, bucket.isZero() ? Duration.ofSeconds(1) : bucket));
            writeAll(List.of(e));
            ctx.cursorStore().save(config.sourceId(), PollCursor.at(now.toString()));
            return false;
        }

        private boolean pollLog() throws Exception {
            long count = readCounter(master, settings);
            long next = ctx.cursorStore().load(config.sourceId()).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
            if (count < next) {
                next = 0;   // 장비 카운터가 초기화됨
            }
            if (count - next > settings.ringSize) {
                long lost = count - next - settings.ringSize;
                org.slf4j.LoggerFactory.getLogger(ModbusTcpConnector.class)
                        .warn("소스 {}: Modbus 기록 고리가 넘쳐 {}건을 읽지 못했습니다", config.sourceId(), lost);
                next = count - settings.ringSize;
            }
            if (count == next) {
                return false;
            }
            long until = Math.min(count, next + Math.max(1, MAX_REGISTERS / settings.recordLength) * 4L);
            List<RawEnvelope> batch = new ArrayList<>();
            for (long i = next; i < until; i++) {
                int slot = (int) (i % settings.ringSize);
                int[] w = words(master.readMultipleRegisters(settings.unitId,
                        settings.recordAddress + slot * settings.recordLength, settings.recordLength));
                ObjectNode o = JSON.createObjectNode().put("unitId", settings.unitId);
                ArrayNode regs = o.putArray("registers");
                for (int v : w) {
                    regs.add(v);
                }
                batch.add(envelope(settings.topic, JSON.writeValueAsBytes(o)));
            }
            if (isPaused()) {
                return false;
            }
            writeAll(batch);   // 실패하면 위치를 저장하지 않는다
            ctx.cursorStore().save(config.sourceId(), PollCursor.at(Long.toString(until)));
            return true;
        }

        @Override
        protected void disconnect() {
            if (master != null) {
                master.disconnect();
                master = null;
            }
        }
    }
}
