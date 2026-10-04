package net.java21.data2flow.ingress.connector.onem2m;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/**
 * oneM2M CSE(KETI Mobius) HTTP 바인딩을 흉내 낸 시험 서버. Mobius 컨테이너 이미지는 MySQL이 따로 필요하고 공식 이미지가 없어 흉내 서버로
 * 시험한다. 지원: CSEBase 조회, {@code /la}, 컨테이너 {@code ?rcn=4&ty=4&stb=&lim=}(stateTag 큰 것만, 개수 제한). X-M2M-Origin이 없으면 403.
 * 확인 수 = 커넥터가 저장한 마지막 stateTag.
 */
final class MobiusMockCse implements ContractPeer, AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    final List<String> cins = new CopyOnWriteArrayList<>();
    private final HttpServer server;
    private final InMemoryPollCursorStore store;
    private final long sourceId;

    MobiusMockCse(InMemoryPollCursorStore store, long sourceId) throws IOException {
        this.store = store;
        this.sourceId = sourceId;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/Mobius", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    String cseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/Mobius";
    }

    @Override
    public void publish(List<byte[]> payloads) {
        payloads.forEach(p -> cins.add(new String(p, StandardCharsets.UTF_8)));
    }

    @Override
    public long acknowledgedCount() {
        return store.load(sourceId).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            if (ex.getRequestHeaders().getFirst("X-M2M-Origin") == null) {
                ex.getResponseHeaders().add("X-M2M-RSC", "4103");
                ex.sendResponseHeaders(403, -1);
                return;
            }
            String path = ex.getRequestURI().getPath();
            Map<String, String> q = new HashMap<>();
            if (ex.getRequestURI().getRawQuery() != null) {
                for (String kv : ex.getRequestURI().getRawQuery().split("&")) {
                    String[] p = kv.split("=", 2);
                    q.put(p[0], p.length > 1 ? p[1] : "");
                }
            }
            ObjectNode body = JSON.createObjectNode();
            if (path.equals("/Mobius")) {
                body.putObject("m2m:cb").put("rn", "Mobius").put("ty", 5);
            } else if (path.equals("/Mobius/kit-ae/kit-cnt/la")) {
                if (cins.isEmpty()) {
                    ex.getResponseHeaders().add("X-M2M-RSC", "4004");
                    ex.sendResponseHeaders(404, -1);
                    return;
                }
                cin(body.putObject("m2m:cin"), cins.size());
            } else if (path.equals("/Mobius/kit-ae/kit-cnt") && !"4".equals(q.get("rcn"))) {
                body.putObject("m2m:cnt").put("rn", "kit-cnt").put("ty", 3).put("st", cins.size()).put("cni", cins.size());
            } else if (path.equals("/Mobius/kit-ae/kit-cnt")) {
                long stb = Long.parseLong(q.getOrDefault("stb", "0"));
                int lim = Integer.parseInt(q.getOrDefault("lim", "100"));
                ObjectNode cnt = body.putObject("m2m:cnt").put("rn", "kit-cnt").put("ty", 3).put("st", cins.size());
                ArrayNode list = cnt.putArray("m2m:cin");
                for (int st = (int) stb + 1; st <= cins.size() && list.size() < lim; st++) {
                    cin(list.addObject(), st);
                }
            } else {
                ex.getResponseHeaders().add("X-M2M-RSC", "4004");
                ex.sendResponseHeaders(404, -1);
                return;
            }
            byte[] out = JSON.writeValueAsBytes(body);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.getResponseHeaders().add("X-M2M-RSC", "2000");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
        }
    }

    private void cin(ObjectNode o, int st) {
        o.put("rn", "4-" + st).put("ty", 4).put("ri", "/Mobius/kit-ae/kit-cnt/4-" + st).put("st", st)
                .put("ct", "20261004T000000").put("cs", cins.get(st - 1).length()).put("con", cins.get(st - 1));
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
