package net.java21.data2flow.ingress.payload;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Apicurio Registry(ccompat v7)·Confluent 호환 스키마 레지스트리 스텁: {@code GET /apis/ccompat/v7/schemas/ids/{id}}.
 * 등록한 ID는 200 {@code {"schema": …}}, 상태 코드를 지정한 ID는 그 코드, 나머지는 404(TC-DSC-284 "Apicurio 레지스트리 스텁").
 */
public final class AvroRegistryStub implements AutoCloseable {

    public static final String BASE = "/apis/ccompat/v7";
    private final HttpServer server;
    private final Map<Integer, String> bodies = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> statuses = new ConcurrentHashMap<>();
    private final AtomicInteger requests = new AtomicInteger();

    public AvroRegistryStub() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext(BASE + "/schemas/ids/", exchange -> {
            requests.incrementAndGet();
            String path = exchange.getRequestURI().getPath();
            int id = Integer.parseInt(path.substring(path.lastIndexOf('/') + 1));
            Integer status = statuses.get(id);
            String body = bodies.get(id);
            int code = status != null ? status : body != null ? 200 : 404;
            byte[] bytes = (code == 200 ? body : "{\"error_code\":40403,\"message\":\"Schema not found\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    /** Avro 스키마를 ID로 등록한다 */
    public AvroRegistryStub avro(int id, String avsc) {
        bodies.put(id, "{\"schema\":" + PayloadTestData.JSON.writeValueAsString(avsc) + "}");
        return this;
    }

    /** 응답 본문을 그대로 */
    public AvroRegistryStub raw(int id, String body) {
        bodies.put(id, body);
        return this;
    }

    public AvroRegistryStub status(int id, int code) {
        statuses.put(id, code);
        return this;
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + BASE;
    }

    public int requests() {
        return requests.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
