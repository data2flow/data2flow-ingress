package net.java21.data2flow.ingress.connector.http;

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
import net.java21.data2flow.ingress.connector.common.ConnectorTls;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HTTP 폴링 커넥터(키 {@code http-poll}, DSC-09.09, BR-DSC-24, TC-DSC-246). 주기(10초 이상)마다 REST GET을 부르고, 응답 JSON의 배열
 * ({@code itemsPath})의 항목 하나를 원본 하나로 넘긴다.
 *
 * <ul>
 *   <li><b>증분 기준:</b> 마지막 커서({@code nextCursorPath} 응답 값 또는 마지막 항목의 {@code cursorField})를 {@code cursorParam}으로
 *       보낸다. ETag만 있는 API는 {@code etag: true}로 {@code If-None-Match}를 보내고 304면 건너뛴다.</li>
 *   <li><b>페이지네이션:</b> {@code nextPageTokenPath}가 있으면 같은 증분 기준 안에서 다음 페이지를 이어 읽는다(PollCursor.pageToken).</li>
 *   <li><b>무손실:</b> 한 페이지를 모두 기록(confirm)한 뒤에만 위치를 저장한다(CURSOR). 재시작하면 저장된 위치부터 다시 읽고, 리스를 잃으면
 *       저장이 거부되어 멈춘다(BR-DSC-26).</li>
 *   <li>리더 1대만 실행한다(SINGLETON). 쓰기 요청을 보내지 않는다(GET만).</li>
 * </ul>
 */
public class HttpPollingConnector implements SourceConnector {

    public static final String KEY = "http-poll";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    static final JsonMapper JSON = JsonMapper.builder().build();

    private final PollingOptions options;
    private final Clock clock;

    public HttpPollingConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "HTTP 폴링", "1.0.0", ConnectorCategory.HTTP,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.TOKEN, AuthMethod.OAUTH2_CC, AuthMethod.MTLS),
                Set.of(PayloadFormat.JSON), AckMode.CURSOR, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config, options);
        HttpClient http = client(config);
        HttpAuth auth = HttpAuth.from(Cfg.of(config), http, clock);
        int port = s.url.getPort() > 0 ? s.url.getPort() : "https".equals(s.url.getScheme()) ? 443 : 80;
        javax.net.ssl.SSLContext ssl = null;
        if ("https".equals(s.url.getScheme())) {
            try {
                ssl = ConnectorTls.from(Cfg.of(config)).context();
            } catch (Exception ignored) {
                ssl = null;
            }
        }
        return new StagedConnectionTest<AutoCloseable>(s.url.getHost(), port, false, ssl,
                !ConnectorTls.from(Cfg.of(config)).insecure(), options.testTimeout(), "POLL")
                .run(remaining -> {
                    auth.apply(HttpRequest.newBuilder(s.url));   // 토큰 받기(OAuth2)
                    return () -> { };
                }, (h, remaining) -> {
                    Page page = fetch(http, auth, s, PollCursor.initial(), remaining);
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    for (byte[] item : page.items) {
                        if (preview.size() < ConnectionTestResult.MAX_PREVIEW) {
                            preview.add(StagedConnectionTest.preview(clock, s.topic, item));
                        }
                    }
                    return preview;
                }, false);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config, options), sink, ctx);
    }

    static HttpClient client(SourceConfig config) {
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL);
        try {
            if ("https".equalsIgnoreCase(URI.create(Cfg.of(config).required("url")).getScheme())) {
                b.sslContext(ConnectorTls.from(Cfg.of(config)).context());
            }
        } catch (InvalidSettingsException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "CA_CERT");
        }
        return b.build();
    }

    /** 한 번 읽은 결과 */
    record Page(List<byte[]> items, String nextCursor, String nextPageToken, boolean notModified) {
    }

    /** GET 한 번 */
    static Page fetch(HttpClient http, HttpAuth auth, Settings s, PollCursor at, Duration timeout) throws Exception {
        Map<String, String> query = new LinkedHashMap<>(s.query);
        if (s.cursorParam != null && at.cursor() != null && !s.etag) {
            query.put(s.cursorParam, at.cursor());
        }
        if (s.pageTokenParam != null && at.pageToken() != null) {
            query.put(s.pageTokenParam, at.pageToken());
        }
        if (s.pageSizeParam != null) {
            query.put(s.pageSizeParam, Integer.toString(s.pageSize));
        }
        StringBuilder uri = new StringBuilder(s.url.toString());
        char sep = s.url.getRawQuery() == null ? '?' : '&';
        for (Map.Entry<String, String> e : query.entrySet()) {
            uri.append(sep).append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
            sep = '&';
        }
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(uri.toString())).timeout(timeout).GET()
                .header("Accept", "application/json");
        s.headers.forEach(b::header);
        if (s.etag && at.cursor() != null) {
            b.header("If-None-Match", at.cursor());
        }
        HttpResponse<byte[]> res = http.send(auth.apply(b).build(), HttpResponse.BodyHandlers.ofByteArray());
        if (res.statusCode() == 304) {
            return new Page(List.of(), at.cursor(), null, true);
        }
        if (res.statusCode() == 401 || res.statusCode() == 403) {
            auth.rejected();
            throw new SecurityException("not authorized HTTP " + res.statusCode());
        }
        if (res.statusCode() / 100 != 2) {
            throw new IOException("HTTP " + res.statusCode());
        }
        JsonNode body = JSON.readTree(res.body());
        JsonNode array = s.itemsPath.isEmpty() ? body : body.at(s.itemsPath);
        if (!array.isArray()) {
            throw new IOException("응답의 " + (s.itemsPath.isEmpty() ? "본문" : s.itemsPath) + "이 배열이 아닙니다");
        }
        List<byte[]> items = new ArrayList<>(array.size());
        String lastCursor = null;
        for (JsonNode item : array) {
            items.add(JSON.writeValueAsBytes(item));
            if (s.cursorField != null) {
                JsonNode c = item.at(s.cursorField);
                if (!c.isMissingNode() && !c.isNull()) {
                    lastCursor = c.asString();
                }
            }
        }
        String next;
        if (s.etag) {
            next = res.headers().firstValue("ETag").orElse(at.cursor());
        } else if (s.nextCursorPath != null && !body.at(s.nextCursorPath).isMissingNode() && !body.at(s.nextCursorPath).isNull()) {
            next = body.at(s.nextCursorPath).asString();
        } else {
            next = lastCursor != null ? lastCursor : at.cursor();
        }
        String pageToken = null;
        if (s.nextPageTokenPath != null) {
            JsonNode t = body.at(s.nextPageTokenPath);
            pageToken = t.isMissingNode() || t.isNull() || t.asString("").isBlank() ? null : t.asString();
        }
        return new Page(items, next, pageToken, false);
    }

    /** 설정({@code http-poll.schema.json}) */
    record Settings(URI url, Map<String, String> query, Map<String, String> headers, Duration interval, String itemsPath,
                    String cursorParam, String cursorField, String nextCursorPath, String pageTokenParam,
                    String nextPageTokenPath, String pageSizeParam, int pageSize, boolean etag, String topic) {
        static Settings from(SourceConfig config, PollingOptions options) {
            Cfg c = Cfg.of(config);
            URI url = c.uri("url");
            if (!Set.of("http", "https").contains(url.getScheme())) {
                throw InvalidSettingsException.config("url");
            }
            Map<String, String> headers = new LinkedHashMap<>();
            c.node().path("headers").properties().forEach(e -> headers.put(e.getKey(), e.getValue().asString()));
            Map<String, String> query = new LinkedHashMap<>();
            c.node().path("query").properties().forEach(e -> query.put(e.getKey(), e.getValue().asString()));
            Duration interval = options.pollInterval(c.seconds("intervalSec", 60, 1, 86_400));
            String items = c.text("itemsPath", "");
            for (String pointer : new String[]{items, c.text("cursorField", ""), c.text("nextCursorPath", ""),
                    c.text("nextPageTokenPath", "")}) {
                if (!pointer.isEmpty() && !pointer.startsWith("/")) {
                    throw InvalidSettingsException.config("itemsPath");
                }
            }
            return new Settings(url, query, headers, interval, items, c.text("cursorParam", null),
                    c.text("cursorField", null), c.text("nextCursorPath", null), c.text("pageTokenParam", null),
                    c.text("nextPageTokenPath", null), c.text("pageSizeParam", null),
                    c.integer("pageSize", 100, 1, 10_000), c.bool("etag", false), c.text("topic", url.getPath()));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private HttpClient http;
        private HttpAuth auth;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), settings.interval, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            http = client(config);
            auth = HttpAuth.from(Cfg.of(config), http, ctx.clock());
            auth.apply(HttpRequest.newBuilder(settings.url));
        }

        @Override
        protected boolean pollOnce() throws Exception {
            PollCursor at = ctx.cursorStore().load(config.sourceId()).orElse(PollCursor.initial());
            Page page = fetch(http, auth, settings, at, Duration.ofSeconds(30));
            if (page.notModified || page.items.isEmpty() && page.nextPageToken == null) {
                if (page.nextCursor != null && !page.nextCursor.equals(at.cursor())) {
                    ctx.cursorStore().save(config.sourceId(), PollCursor.at(page.nextCursor));
                }
                return false;
            }
            if (isPaused()) {
                return false;   // 위치를 저장하지 않았으니 재개하면 같은 곳부터 읽는다
            }
            List<RawEnvelope> batch = new ArrayList<>(page.items.size());
            for (byte[] item : page.items) {
                batch.add(envelope(settings.topic, item));
            }
            writeAll(batch);   // 실패하면 위치를 저장하지 않는다(BR-DSC-24)
            PollCursor next = page.nextPageToken != null ? new PollCursor(at.cursor(), page.nextPageToken)
                    : PollCursor.at(page.nextCursor);
            ctx.cursorStore().save(config.sourceId(), next);
            return !settings.etag;   // 증분 API는 빈 응답이 올 때까지 이어 읽는다
        }

        @Override
        protected void disconnect() {
            http = null;
        }
    }
}
