package net.java21.data2flow.ingress.connector.webhook;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

/** 시험용 HTTP 앞단: {@code POST /ingest/webhook/{key}} → {@link WebhookRouter}(운영은 Spring {@code WebhookController}) */
final class WebhookTestServer implements AutoCloseable {

    private final HttpServer server;

    WebhookTestServer(WebhookRouter router) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ingest/webhook/", ex -> {
            try (ex) {
                String key = ex.getRequestURI().getPath().substring("/ingest/webhook/".length());
                byte[] body = ex.getRequestBody().readAllBytes();
                int status = router.find(key).map(s -> s.receive(h -> ex.getRequestHeaders().getFirst(h), body).status())
                        .orElse(404);
                ex.sendResponseHeaders(status, -1);
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    URI url(String key) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/ingest/webhook/" + key);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
