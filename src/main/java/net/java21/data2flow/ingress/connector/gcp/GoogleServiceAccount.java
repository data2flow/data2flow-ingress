package net.java21.data2flow.ingress.connector.gcp;

import net.java21.data2flow.ingress.connector.common.RefreshingToken;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/**
 * Google 서비스 계정 키(JSON)로 OAuth 2.0 접근 토큰을 받는다(JWT bearer, RFC 7523). 서명은 JDK RS256, 라이브러리 없음.
 * 토큰 갱신 시점은 {@link RefreshingToken}(BR-DSC-27)이 정한다.
 */
final class GoogleServiceAccount implements RefreshingToken.Fetcher {

    static final String SCOPE = "https://www.googleapis.com/auth/pubsub";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final String clientEmail;
    private final PrivateKey key;
    private final URI tokenUri;
    private final HttpClient http;
    private final Clock clock;

    GoogleServiceAccount(String json, HttpClient http, Clock clock) throws Exception {
        JsonNode n = JSON.readTree(json);
        this.clientEmail = n.path("client_email").asString();
        this.tokenUri = URI.create(n.path("token_uri").asString("https://oauth2.googleapis.com/token"));
        String pem = n.path("private_key").asString();
        byte[] der = Base64.getDecoder().decode(pem.replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "").replaceAll("\\s", ""));
        this.key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
        this.http = http;
        this.clock = clock;
    }

    /** 서명한 JWT 단언(assertion) */
    String assertion(Instant now) throws Exception {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        var claims = JSON.createObjectNode().put("iss", clientEmail).put("scope", SCOPE).put("aud", tokenUri.toString())
                .put("iat", now.getEpochSecond()).put("exp", now.plus(Duration.ofHours(1)).getEpochSecond());
        String body = b64.encodeToString(JSON.writeValueAsBytes(claims));
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initSign(key);
        sig.update((header + "." + body).getBytes(StandardCharsets.US_ASCII));
        return header + "." + body + "." + b64.encodeToString(sig.sign());
    }

    @Override
    public RefreshingToken.Token fetch() throws Exception {
        Instant now = clock.instant();
        String form = "grant_type=" + URLEncoder.encode("urn:ietf:params:oauth:grant-type:jwt-bearer", StandardCharsets.UTF_8)
                + "&assertion=" + assertion(now);
        HttpResponse<String> res = http.send(HttpRequest.newBuilder(tokenUri).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            throw new SecurityException("토큰 발급 실패 HTTP " + res.statusCode());
        }
        JsonNode t = JSON.readTree(res.body());
        return new RefreshingToken.Token(t.path("access_token").asString(), now,
                now.plusSeconds(t.path("expires_in").asLong(3600)));
    }
}
