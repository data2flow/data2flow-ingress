package net.java21.data2flow.ingress.connector.webhook;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Webhook 서명(BR-DSC-10, DSC-07.04): {@code X-D2F-Signature = hex(HMAC-SHA256(key, timestamp + "." + body))}. 키는 소스 비밀값
 * {@code HMAC_KEY}의 UTF-8 바이트, hex는 소문자 64자({@code sha256=} 접두사는 허용). 비교는 상수 시간.
 */
public final class WebhookSignature {

    public static final String HEADER_SIGNATURE = "X-D2F-Signature";
    public static final String HEADER_TIMESTAMP = "X-D2F-Timestamp";
    public static final String HEADER_REQUEST_ID = "X-D2F-Request-Id";

    private WebhookSignature() {
    }

    public static String sign(String key, String timestamp, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) '.');
            mac.update(body);
            return HexFormat.of().formatHex(mac.doFinal());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256을 쓸 수 없습니다", e);
        }
    }

    public static boolean verify(String key, String timestamp, byte[] body, String signature) {
        if (signature == null || timestamp == null) {
            return false;
        }
        String given = signature.trim().toLowerCase(java.util.Locale.ROOT);
        if (given.startsWith("sha256=")) {
            given = given.substring(7);
        }
        return MessageDigest.isEqual(sign(key, timestamp, body).getBytes(StandardCharsets.US_ASCII),
                given.getBytes(StandardCharsets.US_ASCII));
    }
}
