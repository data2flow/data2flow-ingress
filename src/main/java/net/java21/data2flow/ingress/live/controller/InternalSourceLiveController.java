package net.java21.data2flow.ingress.live.controller;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.ingress.live.service.LiveTap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * API-DSC-52 {@code GET /internal/ingress/sources/{source-id}/live}(SSE): 소스 원본 메시지 샘플 스트림. 호출자는 core-api(API-DSC-10 중계,
 * 권한 SRC_ADMIN은 core가 검사). 이벤트 {@code message{receivedAt, topic, size, rawExcerpt}}, {@code dropped{count}}.
 */
@RestController
public class InternalSourceLiveController {

    private final LiveTap tap;

    public InternalSourceLiveController(LiveTap tap) {
        this.tap = tap;
    }

    @GetMapping(path = "/internal/ingress/sources/{source-id}/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter live(@PathVariable("source-id") long sourceId,
                           @RequestParam(name = "topicFilter", required = false) String topicFilter) {
        SseEmitter emitter = tap.subscribe(sourceId, topicFilter);
        if (emitter == null) {
            throw new BusinessException(CommonErrorCode.RATE_LIMITED, 5);
        }
        return emitter;
    }
}
