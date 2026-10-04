package net.java21.data2flow.ingress.connector.http;

import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.RefreshingToken;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Locale;

/**
 * HTTP 커넥터 인증(DSC-09.05 인증 방식 매트릭스): NONE, BASIC(사용자 + 비밀값 PASSWORD), BEARER(비밀값 TOKEN), API_KEY(헤더 이름 +
 * 비밀값 API_KEY), OAUTH2_CC(OAuth 2.0 클라이언트 자격증명: tokenUrl·clientId·scope + 비밀값 OAUTH2_CLIENT, 토큰은 만료 전 자동 갱신,
 * BR-DSC-27).
 */
public final class HttpAuth {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final String type;
    private final String headerName;
    private final String staticValue;
    private final RefreshingToken token;

    private HttpAuth(String type, String headerName, String staticValue, RefreshingToken token) {
        this.type = type;
        this.headerName = headerName;
        this.staticValue = staticValue;
        this.token = token;
    }

    public static HttpAuth from(Cfg cfg, HttpClient http, Clock clock) {
        Cfg a = cfg.child("auth");
        String type = (a.node().isObject() ? a.text("type", "NONE") : cfg.text("auth", "NONE")).toUpperCase(Locale.ROOT);
        Cfg src = a.node().isObject() ? a : cfg;
        return switch (type) {
            case "NONE" -> new HttpAuth(type, null, null, null);
            case "BASIC" -> new HttpAuth(type, "Authorization", "Basic " + Base64.getEncoder().encodeToString(
                    (src.required("username") + ":" + cfg.requiredSecret("PASSWORD").reveal()).getBytes(StandardCharsets.UTF_8)),
                    null);
            case "BEARER" -> new HttpAuth(type, "Authorization", "Bearer " + cfg.requiredSecret("TOKEN").reveal(), null);
            case "API_KEY" -> new HttpAuth(type, src.text("headerName", "X-API-Key"), cfg.requiredSecret("API_KEY").reveal(), null);
            case "OAUTH2_CC" -> new HttpAuth(type, "Authorization", null, new RefreshingToken(
                    clientCredentials(URI.create(src.required("tokenUrl")), src.required("clientId"), src.text("scope", null),
                            cfg.requiredSecret("OAUTH2_CLIENT").reveal(), http, clock), clock));
            default -> throw new InvalidSettingsException(InvalidSettingsException.Reason.AUTH_UNSUPPORTED, "auth");
        };
    }

    /** OAuth 2.0 client_credentials 토큰 받기(RFC 6749 §4.4) */
    static RefreshingToken.Fetcher clientCredentials(URI tokenUrl, String clientId, String scope, String secret,
                                                     HttpClient http, Clock clock) {
        return () -> {
            Instant now = clock.instant();
            String form = "grant_type=client_credentials" + (scope == null ? "" : "&scope=" + URLEncoder.encode(scope, StandardCharsets.UTF_8));
            HttpResponse<String> res = http.send(HttpRequest.newBuilder(tokenUrl).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                            (URLEncoder.encode(clientId, StandardCharsets.UTF_8) + ":" + URLEncoder.encode(secret, StandardCharsets.UTF_8))
                                    .getBytes(StandardCharsets.UTF_8)))
                    .POST(HttpRequest.BodyPublishers.ofString(form)).build(), HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() / 100 != 2) {
                throw new SecurityException("토큰 발급 실패 HTTP " + res.statusCode());
            }
            JsonNode t = JSON.readTree(res.body());
            long expiresIn = t.path("expires_in").asLong(0);
            return new RefreshingToken.Token(t.path("access_token").asString(), now,
                    expiresIn > 0 ? now.plusSeconds(expiresIn) : null);
        };
    }

    /** 요청에 인증 헤더를 붙인다 */
    public HttpRequest.Builder apply(HttpRequest.Builder b) throws Exception {
        if (token != null) {
            return b.header(headerName, "Bearer " + token.get());
        }
        return staticValue == null ? b : b.header(headerName, staticValue);
    }

    /** 401·403을 받으면 토큰을 버리고 다시 받게 한다 */
    public void rejected() {
        if (token != null) {
            token.invalidate();
        }
    }

    public String type() {
        return type;
    }
}
