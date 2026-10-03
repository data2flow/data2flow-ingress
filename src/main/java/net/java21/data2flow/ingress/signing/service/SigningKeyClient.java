package net.java21.data2flow.ingress.signing.service;

import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.ingress.common.IngressProperties;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * core-api 내부 API로 플랫폼 브로커 기기의 ACTIVE 서명 키를 읽는다(API-DSC-72
 * {@code GET /internal/core/device-credentials/signing-keys}, ADR-042). 토큰 없이 {@code X-CALLER-SERVICE}만 붙인다(ADR-021).
 * <b>응답에 서명 키 원문이 있으므로 본문을 로그에 남기지 않는다.</b>
 */
public class SigningKeyClient implements SigningKeyCache.Fetcher {

    static final String PATH = "/internal/core/device-credentials/signing-keys";
    static final String CALLER = "data2flow-ingress";

    private final RestClient rest;
    private final JsonMapper json;

    public SigningKeyClient(RestClient.Builder builder, JsonMapper json, IngressProperties properties) {
        this.rest = builder.baseUrl(properties.coreUri()).build();
        this.json = json;
    }

    @Override
    public List<SigningKeyCache.Key> fetch() {
        byte[] body = rest.get().uri(PATH).header(DataflowHeaders.CALLER_SERVICE, CALLER).retrieve().body(byte[].class);
        return parse(body == null || body.length == 0 ? json.createObjectNode() : json.readTree(body));
    }

    static List<SigningKeyCache.Key> parse(JsonNode root) {
        JsonNode body = root.has("response") && root.get("response").isObject() ? root.get("response") : root;
        List<SigningKeyCache.Key> keys = new ArrayList<>();
        for (JsonNode k : body.path("keys")) {
            long sourceId = number(k.get("sourceId"));
            String deviceKey = k.path("deviceKey").asString("");
            String signingKey = k.path("signingKey").asString("");
            if (sourceId < 1 || deviceKey.isBlank() || signingKey.isBlank()) {
                continue;
            }
            Instant expiresAt = null;
            if (k.hasNonNull("expiresAt")) {
                try {
                    expiresAt = Instant.parse(k.get("expiresAt").asString());
                } catch (RuntimeException e) {
                    expiresAt = null;
                }
            }
            keys.add(new SigningKeyCache.Key(sourceId, deviceKey.toLowerCase(Locale.ROOT), number(k.get("deviceId")),
                    number(k.get("credentialId")), signingKey, expiresAt));
        }
        return keys;
    }

    private static long number(JsonNode n) {
        if (n == null || n.isNull()) {
            return 0;
        }
        if (n.isNumber()) {
            return n.asLong();
        }
        try {
            return Long.parseLong(n.asString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
