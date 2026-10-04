package net.java21.data2flow.ingress.webhook.controller;

import jakarta.servlet.http.HttpServletRequest;
import net.java21.data2flow.contracts.web.ApiHeader;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.ingress.connector.webhook.WebhookConnector;
import net.java21.data2flow.ingress.connector.webhook.WebhookRouter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Webhook 수신(API-DSC-54, DSC-01.03·07.04). 외부 공개 호스트 {@code data2flow-hook.java21.net}에서 호스트 nginx → 클러스터 Ingress가
 * {@code /ingest/webhook/**}만 이 서비스로 넘긴다(ADR-029). gateway를 거치지 않으므로 신원 헤더가 없고, 소스 서명(HMAC)으로만 인증한다.
 * 응답: 202 기록 확인 뒤 {@code {requestId, receivedAt}}, 401 서명·시각, 409 재생, 413 크기, 503 기록 실패·일시정지(다시 보내기), 404 없는 키.
 */
@RestController
public class WebhookController {

    private final WebhookRouter router;

    public WebhookController(WebhookRouter router) {
        this.router = router;
    }

    @PostMapping("/ingest/webhook/{source-key}")
    public ResponseEntity<ApiResponse<Map<String, Object>>> receive(@PathVariable("source-key") String sourceKey,
                                                                    @RequestBody(required = false) byte[] body,
                                                                    HttpServletRequest request) {
        WebhookConnector.Session session = router.find(sourceKey).orElse(null);
        if (session == null) {
            return error(HttpStatus.NOT_FOUND, "RESOURCE_NOT_FOUND", "Webhook 소스를 찾을 수 없습니다");
        }
        WebhookConnector.Result r = session.receive(request::getHeader, body == null ? new byte[0] : body);
        if (r.status() == 202) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("requestId", r.requestId());
            out.put("receivedAt", r.receivedAt().toString());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(ApiResponse.success(out));
        }
        String message = switch (r.code()) {
            case "WEBHOOK_SIGNATURE_INVALID" -> "서명이 맞지 않습니다";
            case "WEBHOOK_TIMESTAMP_INVALID" -> "요청 시각이 허용 범위(5분)를 벗어났습니다";
            case "WEBHOOK_REPLAYED" -> "이미 받은 요청입니다";
            case "PAYLOAD_TOO_LARGE" -> "본문이 256KB를 넘습니다";
            case "INVALID_REQUEST" -> "X-D2F-Request-Id가 없습니다";
            default -> "잠시 후 다시 보내 주세요";
        };
        ResponseEntity<ApiResponse<Map<String, Object>>> res = error(HttpStatus.valueOf(r.status()), r.code(), message);
        if (r.status() == 503) {
            return ResponseEntity.status(503).header("Retry-After", "5").body(res.getBody());
        }
        return res;
    }

    private static ResponseEntity<ApiResponse<Map<String, Object>>> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(new ApiResponse<>(ApiHeader.failure(code, message), null));
    }
}
