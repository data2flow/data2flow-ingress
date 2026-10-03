package net.java21.data2flow.ingress.support;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * core-api 내부 API 대역(API-DSC-50 {@code GET /internal/core/sources/runtime-config}). 테스트가 응답 본문을 바꾼다.
 * {@code sinceVersion}이 지금 버전과 같으면 204를 준다.
 */
public final class CoreStub implements AutoCloseable {

    private final HttpServer server;
    private final AtomicReference<String> body = new AtomicReference<>("{\"version\":\"0\",\"sources\":[]}");
    private final AtomicReference<String> version = new AtomicReference<>("0");
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> lastCaller = new AtomicReference<>();

    public CoreStub() {
        try {
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/internal/core/sources/runtime-config", exchange -> {
            requests.incrementAndGet();
            lastCaller.set(exchange.getRequestHeaders().getFirst("X-CALLER-SERVICE"));
            String query = exchange.getRequestURI().getQuery();
            if (query != null && query.contains("sinceVersion=" + version.get())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    public String uri() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    /** 응답을 바꾼다. sourcesJson은 sources 배열 JSON */
    public void sources(String version, String sourcesJson) {
        this.version.set(version);
        this.body.set("{\"version\":\"" + version + "\",\"sources\":" + sourcesJson + "}");
    }

    public int requests() {
        return requests.get();
    }

    public String lastCaller() {
        return lastCaller.get();
    }

    /** MQTT 구독 소스 하나(API-DSC-50 모양) */
    public static String mqttSource(long id, String lifecycle, String url, String topic, String extraConfig) {
        return """
                {"id":%d,"organizationId":1,"type":"MQTT","lifecycle":"%s",
                 "config":{"url":"%s","version":"5.0","keepaliveSec":30,"cleanStart":false,"sessionExpirySec":600%s},
                 "secrets":{},"topics":[{"topic":"%s","qos":1}],"qos":1,"clientId":"data2flow-ingress",
                 "unknownDevicePolicy":"AUTO_REGISTER","rateLimit":500}
                """.formatted(id, lifecycle, url, extraConfig == null ? "" : "," + extraConfig, topic);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
