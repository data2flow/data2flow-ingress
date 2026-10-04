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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/**
 * 계약 IT의 상대 SSE 서버(실제 HTTP, JDK). 레코드 i를 {@code id: i+1}, {@code event: kit}로 보내고, {@code Last-Event-ID}가 오면 그 뒤부터
 * 보낸다. 새 레코드가 생기면 열린 연결로 바로 민다. 확인 수 = 커넥터가 저장한 마지막 이벤트 id.
 */
final class SseTestServer implements ContractPeer, AutoCloseable {

    private final List<byte[]> records = new CopyOnWriteArrayList<>();
    private final Object changed = new Object();
    private final HttpServer server;
    private final InMemoryPollCursorStore store;
    private final long sourceId;
    private volatile boolean closed;

    SseTestServer(InMemoryPollCursorStore store, long sourceId) {
        this.store = store;
        this.sourceId = sourceId;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/events", this::events);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/events";
    }

    @Override
    public void publish(List<byte[]> payloads) {
        records.addAll(payloads);
        synchronized (changed) {
            changed.notifyAll();
        }
    }

    @Override
    public long acknowledgedCount() {
        return store.load(sourceId).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
    }

    private void events(HttpExchange ex) throws IOException {
        String last = ex.getRequestHeaders().getFirst("Last-Event-ID");
        int next = last == null ? 0 : Integer.parseInt(last);
        ex.getResponseHeaders().add("Content-Type", "text/event-stream");
        ex.sendResponseHeaders(200, 0);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(": hello\n\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            while (!closed) {
                while (next < records.size()) {
                    String data = new String(records.get(next), StandardCharsets.UTF_8);
                    next++;
                    out.write(("id: " + next + "\nevent: kit\ndata: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
                }
                out.flush();
                synchronized (changed) {
                    if (next >= records.size()) {
                        changed.wait(200);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            // 클라이언트가 끊음
        }
    }

    @Override
    public void close() {
        closed = true;
        server.stop(0);
    }
}
