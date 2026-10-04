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
import net.java21.data2flow.ingress.connector.common.WriteFailedException;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * HTTP SSE(Server-Sent Events) 스트림 구독 커넥터(키 {@code http-sse}, DSC-09 카탈로그 "SSE/WebSocket 스트림 구독"). 이벤트의 {@code data}
 * 하나를 원본 하나로 넘긴다(주제 = {@code event} 이름 또는 설정 {@code topic}).
 *
 * <p>무손실: 이벤트를 기록(confirm)한 뒤에만 마지막 이벤트 {@code id}를 폴링 위치로 저장하고(CURSOR), 다시 연결할 때
 * {@code Last-Event-ID}로 보내 그 뒤부터 받는다(HTML Living Standard §9.2). 기록이 실패하면 저장한 위치부터 다시 받도록 연결을 새로 맺는다.
 * 이벤트에 {@code id}가 없는 서버는 끊긴 동안의 이벤트를 다시 받을 수 없어 유실 가능이다. 세션 하나라 리더 1대만 실행한다(SINGLETON).
 */
public class SseConnector implements SourceConnector {

    public static final String KEY = "http-sse";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);
    private static final Event END = new Event(null, null, null);

    private final PollingOptions options;
    private final Clock clock;

    public SseConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "HTTP SSE 스트림", "1.0.0", ConnectorCategory.HTTP,
                Set.of(AuthMethod.NONE, AuthMethod.USER_PASSWORD, AuthMethod.TOKEN, AuthMethod.OAUTH2_CC, AuthMethod.MTLS),
                Set.of(PayloadFormat.JSON, PayloadFormat.TEXT), AckMode.CURSOR, ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    /** SSE 이벤트 하나 */
    record Event(String id, String name, String data) {
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config);
        HttpClient http = HttpPollingConnector.client(config);
        HttpAuth auth = HttpAuth.from(Cfg.of(config), http, clock);
        int port = s.url.getPort() > 0 ? s.url.getPort() : "https".equals(s.url.getScheme()) ? 443 : 80;
        return new StagedConnectionTest<Stream>(s.url.getHost(), port, false, null,
                !ConnectorTls.from(Cfg.of(config)).insecure(), options.testTimeout(), ConnectionTestResult.STEP_SUBSCRIBE)
                .run(remaining -> open(http, auth, s, null), (stream, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    long deadline = System.nanoTime() + Math.min(remaining.toNanos(), 3_000_000_000L);
                    while (preview.size() < ConnectionTestResult.MAX_PREVIEW && System.nanoTime() < deadline) {
                        Event e = stream.events.poll(100, TimeUnit.MILLISECONDS);
                        if (e == END) {
                            break;
                        }
                        if (e != null) {
                            preview.add(StagedConnectionTest.preview(clock, e.name() == null ? s.topic : e.name(),
                                    e.data().getBytes(StandardCharsets.UTF_8)));
                        }
                    }
                    return preview;
                }, true);
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config), sink, ctx);
    }

    /** 열린 스트림과 이벤트를 읽는 가상 스레드 */
    static final class Stream implements AutoCloseable {
        final BlockingQueue<Event> events = new ArrayBlockingQueue<>(10_000);
        private final InputStream body;
        private final Thread reader;

        Stream(InputStream body) {
            this.body = body;
            this.reader = Thread.ofVirtual().name("sse-reader").start(this::read);
        }

        private void read() {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
                String id = null;
                String name = null;
                StringBuilder data = null;
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.isEmpty()) {
                        if (data != null) {
                            events.put(new Event(id, name, data.toString()));
                        }
                        name = null;
                        data = null;
                        id = null;
                        continue;
                    }
                    if (line.startsWith(":")) {
                        continue;   // 주석(연결 유지)
                    }
                    int colon = line.indexOf(':');
                    String field = colon < 0 ? line : line.substring(0, colon);
                    String value = colon < 0 ? "" : line.substring(colon + 1).replaceFirst("^ ", "");
                    switch (field) {
                        case "data" -> data = data == null ? new StringBuilder(value) : data.append('\n').append(value);
                        case "id" -> id = value;
                        case "event" -> name = value;
                        default -> { }
                    }
                }
            } catch (IOException e) {
                // 끊김: 아래에서 끝 표시
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            events.offer(END);
        }

        @Override
        public void close() {
            reader.interrupt();
            try {
                body.close();
            } catch (IOException ignored) {
                // 닫는 중 오류는 무시
            }
        }
    }

    static Stream open(HttpClient http, HttpAuth auth, Settings s, String lastEventId) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(s.url).GET().header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache");
        s.headers.forEach(b::header);
        if (lastEventId != null) {
            b.header("Last-Event-ID", lastEventId);
        }
        HttpResponse<InputStream> res = http.send(auth.apply(b).build(), HttpResponse.BodyHandlers.ofInputStream());
        if (res.statusCode() == 401 || res.statusCode() == 403) {
            res.body().close();
            auth.rejected();
            throw new SecurityException("not authorized HTTP " + res.statusCode());
        }
        if (res.statusCode() / 100 != 2) {
            res.body().close();
            throw new IOException("HTTP " + res.statusCode());
        }
        return new Stream(res.body());
    }

    record Settings(URI url, Map<String, String> headers, String topic, int batchSize) {
        static Settings from(SourceConfig config) {
            Cfg c = Cfg.of(config);
            URI url = c.uri("url");
            if (!Set.of("http", "https").contains(url.getScheme())) {
                throw InvalidSettingsException.config("url");
            }
            Map<String, String> headers = new LinkedHashMap<>();
            c.node().path("headers").properties().forEach(e -> headers.put(e.getKey(), e.getValue().asString()));
            return new Settings(url, headers, c.text("topic", url.getPath()), c.integer("batchSize", 100, 1, 1000));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private Stream stream;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), Duration.ZERO, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() throws Exception {
            HttpClient http = HttpPollingConnector.client(config);
            String last = ctx.cursorStore().load(config.sourceId()).map(PollCursor::cursor).orElse(null);
            stream = open(http, HttpAuth.from(Cfg.of(config), http, ctx.clock()), settings, last);
        }

        @Override
        protected boolean pollOnce() throws Exception {
            Event first = stream.events.poll(Math.max(50, options.idleInterval().toMillis()), TimeUnit.MILLISECONDS);
            if (first == null) {
                return false;
            }
            List<Event> events = new ArrayList<>();
            events.add(first);
            stream.events.drainTo(events, settings.batchSize - 1);
            boolean ended = events.remove(END);
            List<RawEnvelope> batch = new ArrayList<>(events.size());
            String lastId = null;
            for (Event e : events) {
                batch.add(envelope(e.name() == null || e.name().isBlank() ? settings.topic : e.name(),
                        e.data().getBytes(StandardCharsets.UTF_8)));
                if (e.id() != null && !e.id().isBlank()) {
                    lastId = e.id();
                }
            }
            if (!batch.isEmpty()) {
                if (isPaused()) {
                    throw new IOException("일시정지: 저장한 위치부터 다시 받는다");
                }
                try {
                    writeAll(batch);
                } catch (WriteFailedException e) {
                    throw new IOException("기록 실패: 저장한 위치부터 다시 받는다", e);
                }
                if (lastId != null) {
                    ctx.cursorStore().save(config.sourceId(), PollCursor.at(lastId));
                }
            }
            if (ended) {
                throw new IOException("SSE 스트림이 끝났습니다");
            }
            return true;
        }

        @Override
        protected void disconnect() {
            if (stream != null) {
                stream.close();
                stream = null;
            }
        }
    }
}
