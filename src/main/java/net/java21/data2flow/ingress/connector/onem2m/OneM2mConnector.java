package net.java21.data2flow.ingress.connector.onem2m;

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
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * oneM2M 커넥터(키 {@code onem2m}, 소스 유형 ONEM2M, DSC-05.01, TS-0001·TS-0004·TS-0009 HTTP 바인딩, 참고: KETI Mobius). 라이브러리 없이
 * HTTP 바인딩을 직접 부른다(connectors.md §2).
 *
 * <p>컨테이너(cnt)마다 주기적으로 {@code GET {컨테이너}?rcn=4&ty=4&stb={마지막 stateTag}&lim={n}}으로 새 contentInstance(cin)를 받아
 * {@code con} 하나를 원본 하나로 넘긴다(문자열이면 그대로, 객체면 JSON). 모두 기록(confirm)한 뒤에 컨테이너별 마지막 {@code st}를 위치로
 * 저장한다(CURSOR, 무손실). 리더 1대만 실행한다(SINGLETON). cin을 만들거나 지우지 않는다.
 */
public class OneM2mConnector implements SourceConnector {

    public static final String KEY = "onem2m";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final PollingOptions options;
    private final Clock clock;

    public OneM2mConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "oneM2M (Mobius)", "1.0.0", ConnectorCategory.INDUSTRIAL,
                Set.of(AuthMethod.NONE, AuthMethod.TOKEN), Set.of(PayloadFormat.JSON, PayloadFormat.TEXT),
                AckMode.CURSOR, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config, options);
        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build();
        int port = s.cse.getPort() > 0 ? s.cse.getPort() : "https".equals(s.cse.getScheme()) ? 443 : 80;
        return new StagedConnectionTest<AutoCloseable>(s.cse.getHost(), port, false, null, true, options.testTimeout(), "POLL")
                .run(remaining -> {
                    get(http, s, config, s.cse.toString());   // CSEBase 조회(인증)
                    return () -> { };
                }, (h, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    for (String c : s.containers) {
                        get(http, s, config, s.cse + "/" + c);   // 컨테이너가 있는가
                        JsonNode la;
                        try {
                            la = get(http, s, config, s.cse + "/" + c + "/la").path("m2m:cin");
                        } catch (IOException noLatest) {
                            continue;   // 아직 contentInstance가 없다(Mobius 404)
                        }
                        if (!la.isMissingNode() && preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                            preview.add(StagedConnectionTest.preview(clock, c, content(la)));
                        }
                    }
                    return preview;
                }, false);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config, options), sink, ctx);
    }

    /** {@code con} → payload */
    static byte[] content(JsonNode cin) {
        JsonNode con = cin.path("con");
        return con.isString() ? con.asString().getBytes(StandardCharsets.UTF_8) : JSON.writeValueAsBytes(con);
    }

    static JsonNode get(HttpClient http, Settings s, SourceConfig config, String uri) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(30)).GET()
                .header("Accept", "application/json").header("X-M2M-Origin", s.origin)
                .header("X-M2M-RI", "d2f-" + UUID.randomUUID()).header("X-M2M-RVI", "3");
        String token = Cfg.of(config).reveal("TOKEN");
        if (token != null) {
            b.header("Authorization", "Bearer " + token);
        }
        HttpResponse<byte[]> res = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
        if (res.statusCode() == 401 || res.statusCode() == 403) {
            throw new SecurityException("not authorized HTTP " + res.statusCode());
        }
        if (res.statusCode() / 100 != 2) {
            throw new IOException("oneM2M HTTP " + res.statusCode() + " RSC " + res.headers().firstValue("X-M2M-RSC").orElse("-"));
        }
        return JSON.readTree(res.body());
    }

    record Settings(URI cse, String origin, List<String> containers, int batch, Duration interval) {
        static Settings from(SourceConfig config, PollingOptions options) {
            Cfg c = Cfg.of(config);
            URI cse = c.uri("cseUrl");
            if (!Set.of("http", "https").contains(cse.getScheme())) {
                throw InvalidSettingsException.config("cseUrl");
            }
            List<String> containers = c.strings("containers");
            if (containers.isEmpty() || containers.size() > 50
                    || containers.stream().anyMatch(x -> !x.matches("[A-Za-z0-9_.~-]+(/[A-Za-z0-9_.~-]+)*"))) {
                throw InvalidSettingsException.config("containers");
            }
            String cseUrl = cse.toString().replaceAll("/$", "");
            return new Settings(URI.create(cseUrl), c.text("origin", "SData2flow"), List.copyOf(containers),
                    c.integer("batchSize", 100, 1, 1000), options.pollInterval(c.seconds("intervalSec", 60, 1, 86_400)));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private HttpClient http;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), settings.interval, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(options.connectTimeout()).build();
            get(http, settings, config, settings.cse.toString());
        }

        /** 위치: 컨테이너 순서대로 마지막 stateTag를 쉼표로 이은 값(예: {@code 12,7}) */
        private long[] load() {
            long[] tags = new long[settings.containers.size()];
            ctx.cursorStore().load(config.sourceId()).map(PollCursor::cursor).ifPresent(c -> {
                String[] parts = c.split(",");
                for (int i = 0; i < Math.min(parts.length, tags.length); i++) {
                    tags[i] = Long.parseLong(parts[i]);
                }
            });
            return tags;
        }

        @Override
        protected boolean pollOnce() throws Exception {
            long[] tags = load();
            boolean more = false;
            for (int i = 0; i < settings.containers.size(); i++) {
                String container = settings.containers.get(i);
                JsonNode res = get(http, settings, config, settings.cse + "/" + container + "?rcn=4&ty=4&stb=" + tags[i]
                        + "&lim=" + settings.batch);
                JsonNode cins = res.path("m2m:cnt").path("m2m:cin");
                if (!cins.isArray() || cins.isEmpty()) {
                    continue;
                }
                List<JsonNode> sorted = new ArrayList<>();
                cins.forEach(sorted::add);
                sorted.sort(Comparator.comparingLong(n -> n.path("st").asLong()));
                List<RawEnvelope> batch = new ArrayList<>();
                long newest = tags[i];
                for (JsonNode cin : sorted) {
                    long st = cin.path("st").asLong();
                    if (st <= tags[i]) {
                        continue;
                    }
                    batch.add(envelope(container, content(cin)));
                    newest = Math.max(newest, st);
                }
                if (batch.isEmpty() || isPaused()) {
                    continue;
                }
                writeAll(batch);   // 실패하면 위치를 저장하지 않는다
                tags[i] = newest;
                StringBuilder sb = new StringBuilder();
                for (int k = 0; k < tags.length; k++) {
                    sb.append(k == 0 ? "" : ",").append(tags[k]);
                }
                ctx.cursorStore().save(config.sourceId(), PollCursor.at(sb.toString()));
                more |= sorted.size() >= settings.batch;
            }
            return more;
        }

        @Override
        protected void disconnect() {
            http = null;
        }
    }
}
