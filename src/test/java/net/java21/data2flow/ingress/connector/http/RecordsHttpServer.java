package net.java21.data2flow.ingress.connector.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/**
 * 계약 IT의 상대 REST API(실제 HTTP 서버, JDK). 추가만 되는 레코드 목록을 {@code GET /records?since=N&limit=M}으로 돌려준다:
 * {@code {"items":[…], "next":"N+k"}}. 확인 수 = 커넥터가 저장한 폴링 위치({@code next}).
 */
final class RecordsHttpServer implements ContractPeer, AutoCloseable {

    final List<byte[]> records = new CopyOnWriteArrayList<>();
    private final HttpServer server;
    private final InMemoryPollCursorStore store;
    private final long sourceId;
    volatile int requests;

    RecordsHttpServer(InMemoryPollCursorStore store, long sourceId) {
        this.store = store;
        this.sourceId = sourceId;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/records", this::records);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/records";
    }

    @Override
    public void publish(List<byte[]> payloads) {
        records.addAll(payloads);
    }

    @Override
    public long acknowledgedCount() {
        return store.load(sourceId).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
    }

    private void records(HttpExchange ex) throws IOException {
        requests++;
        Map<String, String> q = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                String[] p = kv.split("=", 2);
                q.put(p[0], p.length > 1 ? p[1] : "");
            }
        }
        int since = Integer.parseInt(q.getOrDefault("since", "0"));
        int limit = Integer.parseInt(q.getOrDefault("limit", "100"));
        int to = Math.min(records.size(), since + limit);
        StringBuilder sb = new StringBuilder("{\"items\":[");
        for (int i = since; i < to; i++) {
            if (i > since) {
                sb.append(',');
            }
            sb.append(new String(records.get(i), StandardCharsets.UTF_8));
        }
        sb.append("],\"next\":\"").append(Math.max(since, to)).append("\"}");
        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
