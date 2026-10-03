package net.java21.data2flow.ingress.connector.mqtt;

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
import net.java21.data2flow.contracts.messaging.ClientIds;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

/**
 * MQTT 3.1.1·5.0 구독 커넥터(키 {@code mqtt}, DSC-01.04·01.05·09.02·09.03·09.04). tcp·ssl·ws·wss, 인증 없음·사용자/비밀번호·
 * WebSocket HTTP 헤더(Basic, iot-data.java21.net 방식)·mTLS를 지원한다. ChirpStack v4({@code application/+/device/+/event/up})와
 * 플랫폼 브로커({@code devices/+/telemetry}) 소스가 모두 이 커넥터를 쓴다.
 *
 * <p>확인 방식은 AFTER_WRITE(스트림 confirm 뒤 PUBACK), 확장 방식은 기본 DUAL_ACTIVE(ADR-015: 두 인스턴스가 서로 다른 client-id로 같은
 * 토픽을 구독하고 pipeline이 중복을 거른다). MQTT 5 + {@code sharedGroup}이면 SCALABLE(공유 구독으로 나눠 받음, BR-DSC-25).
 *
 * <p><b>구독 전용 커넥터다.</b> 송신(supportsSend)은 false이고 발행 코드가 없다(CLAUDE.md §5, ACT-03.02 결정 대기).
 */
public class MqttSourceConnector implements SourceConnector, AutoCloseable {

    public static final String KEY = "mqtt";
    public static final String VERSION = "1.0.0";
    private static final String SCHEMA_RESOURCE = "/connectors/mqtt.schema.json";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final JsonNode SCHEMA = loadSchema();

    private final MqttConnectorOptions options;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;

    public MqttSourceConnector() {
        this(MqttConnectorOptions.defaults(), Clock.systemUTC());
    }

    public MqttSourceConnector(MqttConnectorOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("mqtt-reconnect").factory());
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "MQTT 3.1.1 / 5.0", VERSION, ConnectorCategory.MQTT,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.WS_HEADER, AuthMethod.MTLS),
                Set.of(PayloadFormat.JSON, PayloadFormat.TEXT, PayloadFormat.BINARY),
                AckMode.AFTER_WRITE, ScalingMode.DUAL_ACTIVE, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        MqttSourceSettings settings = MqttSourceSettings.from(config);
        Duration timeout = options.testTimeout();
        JsonNode requested = config.config().get("testTimeoutSec");
        if (requested != null && requested.isNumber() && requested.asInt() > 0) {
            timeout = Duration.ofSeconds(requested.asInt());
        }
        String base = clientIdBase(config);
        String suffix = HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt());
        return new MqttConnectionTester(settings, ClientIds.test(base, suffix), timeout, clock).run();
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        MqttSourceSettings settings = MqttSourceSettings.from(config);
        String clientId = config.clientId() != null ? config.clientId()
                : ClientIds.test(clientIdBase(config), HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt()));
        return new MqttConnectorSession(config, settings, sink, ctx, options, scheduler, clientId);
    }

    /** 설정으로 정해지는 실제 확장 방식(BR-DSC-25) */
    public ScalingMode scaling(SourceConfig config) {
        return MqttSourceSettings.from(config).scaling();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private static String clientIdBase(SourceConfig config) {
        JsonNode base = config.config().get("clientIdBase");
        String value = base == null || !base.isString() ? "data2flow-ingress" : base.asString();
        return value.matches("[a-z0-9][a-z0-9-]*") ? value : "data2flow-ingress";
    }

    private static JsonNode loadSchema() {
        try (InputStream in = MqttSourceConnector.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("커넥터 스키마가 없습니다: " + SCHEMA_RESOURCE);
            }
            return JSON.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
