package net.java21.data2flow.ingress.payload.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.ingress.common.IngressErrorCode;
import net.java21.data2flow.ingress.payload.dto.SchemaInspectRequest;
import net.java21.data2flow.ingress.payload.dto.SchemaInspectResponse;
import net.java21.data2flow.ingress.payload.dto.TopicTemplatePreviewRequest;
import net.java21.data2flow.ingress.payload.dto.TopicTemplatePreviewResponse;
import net.java21.data2flow.ingress.payload.service.PayloadSchemaInspector;
import net.java21.data2flow.ingress.payload.domain.TopicTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * payload 형식·토픽 템플릿 내부 API(DSC-09.07·09.08). core-api만 부른다(권한 SRC_ADMIN은 core가 검사, ADR-021).
 * <ul>
 *   <li>API-DSC-82 {@code POST /internal/ingress/payload-schemas/inspect}: 올린 .proto·.desc·.avsc를 ingress와 같은 해석기로 검사하고
 *       메시지 타입 목록을 돌려준다. 해석 실패는 400 SOURCE_SCHEMA_INVALID(원인 문구).</li>
 *   <li>API-DSC-83 {@code POST /internal/ingress/topic-templates/preview}: 템플릿을 토픽 몇 개에 맞춰 본다. 템플릿 문법 오류는
 *       400 SOURCE_CONFIG_INVALID({@code topicTemplate}).</li>
 * </ul>
 */
@RestController
public class InternalPayloadController {

    private final PayloadSchemaInspector inspector;

    public InternalPayloadController(PayloadSchemaInspector inspector) {
        this.inspector = inspector;
    }

    @PostMapping("/internal/ingress/payload-schemas/inspect")
    public ApiResponse<SchemaInspectResponse> inspect(@Valid @RequestBody SchemaInspectRequest request) {
        return ApiResponse.success(inspector.inspect(request));
    }

    @PostMapping("/internal/ingress/topic-templates/preview")
    public ApiResponse<TopicTemplatePreviewResponse> preview(@Valid @RequestBody TopicTemplatePreviewRequest request) {
        TopicTemplate template;
        try {
            template = TopicTemplate.compile(request.template());
        } catch (IllegalArgumentException e) {
            throw new BusinessException(IngressErrorCode.SOURCE_CONFIG_INVALID, "topicTemplate: " + e.getMessage());
        }
        List<TopicTemplatePreviewResponse.Result> results = new ArrayList<>();
        for (String topic : request.topics()) {
            Map<String, String> attrs = template.match(topic).orElse(null);
            results.add(new TopicTemplatePreviewResponse.Result(topic, attrs != null, attrs,
                    attrs == null ? IngressStatus.UNMATCHED_TOPIC : null));
        }
        return ApiResponse.success(new TopicTemplatePreviewResponse(template.variables(), template.subscriptionFilter(), results));
    }
}
