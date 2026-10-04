package net.java21.data2flow.ingress.connector.gcp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Google Pub/Sub REST v1 구독 API(pull·acknowledge·modifyAckDeadline)를 흉내 낸 시험 서버(ADR-040: 실제 계정이 없어 흉내 서버로 계약
 * 시험). ack 기한(10초) 안에 ack하지 않으면 다시 보낸다. 확인 수 = acknowledge된 메시지 수.
 */
final class PubSubMockServer implements ContractPeer, AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final long ACK_DEADLINE_NANOS = 10_000_000_000L;

    private record Outstanding(String messageId, byte[] data, long deadline) {
    }

    private final HttpServer server;
    private final Deque<Map.Entry<String, byte[]>> available = new ArrayDeque<>();
    private final Map<String, Outstanding> outstanding = new LinkedHashMap<>();
    private long acked;
    private long nextId;

    PubSubMockServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/projects/kit-project/subscriptions/", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public synchronized void publish(List<byte[]> payloads) {
        for (byte[] p : payloads) {
            available.addLast(Map.entry(Long.toString(++nextId), p));
        }
    }

    @Override
    public synchronized long acknowledgedCount() {
        return acked;
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        byte[] response;
        int status = 200;
        try (ex) {
            if (!path.startsWith("/v1/projects/kit-project/subscriptions/kit-sub")) {
                status = 404;
                response = "{\"error\":{\"code\":404}}".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith(":pull")) {
                JsonNode body = JSON.readTree(ex.getRequestBody().readAllBytes());
                response = pull(body.path("maxMessages").asInt(100));
            } else if (path.endsWith(":acknowledge")) {
                acknowledge(JSON.readTree(ex.getRequestBody().readAllBytes()).path("ackIds"));
                response = "{}".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith(":modifyAckDeadline")) {
                nack(JSON.readTree(ex.getRequestBody().readAllBytes()).path("ackIds"));
                response = "{}".getBytes(StandardCharsets.UTF_8);
            } else {
                response = "{\"name\":\"projects/kit-project/subscriptions/kit-sub\"}".getBytes(StandardCharsets.UTF_8);
            }
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(status, response.length);
            ex.getResponseBody().write(response);
        }
    }

    private synchronized byte[] pull(int max) {
        long now = System.nanoTime();
        outstanding.entrySet().removeIf(e -> {
            if (e.getValue().deadline() < now) {
                available.addFirst(Map.entry(e.getValue().messageId(), e.getValue().data()));
                return true;
            }
            return false;
        });
        ObjectNode res = JSON.createObjectNode();
        ArrayNode list = res.putArray("receivedMessages");
        while (list.size() < max && !available.isEmpty()) {
            Map.Entry<String, byte[]> m = available.pollFirst();
            String ackId = UUID.randomUUID().toString();
            outstanding.put(ackId, new Outstanding(m.getKey(), m.getValue(), now + ACK_DEADLINE_NANOS));
            ObjectNode rm = list.addObject().put("ackId", ackId);
            rm.putObject("message").put("messageId", m.getKey())
                    .put("data", Base64.getEncoder().encodeToString(m.getValue()))
                    .put("publishTime", "2026-10-04T00:00:00Z");
        }
        return JSON.writeValueAsBytes(res);
    }

    private synchronized void acknowledge(JsonNode ids) {
        for (JsonNode id : ids) {
            if (outstanding.remove(id.asString()) != null) {
                acked++;
            }
        }
    }

    private synchronized void nack(JsonNode ids) {
        for (JsonNode id : ids) {
            Outstanding o = outstanding.remove(id.asString());
            if (o != null) {
                available.addFirst(Map.entry(o.messageId(), o.data()));
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
