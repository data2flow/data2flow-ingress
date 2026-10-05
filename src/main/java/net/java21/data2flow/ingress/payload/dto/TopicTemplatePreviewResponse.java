package net.java21.data2flow.ingress.payload.dto;

import java.util.List;
import java.util.Map;

/**
 * API-DSC-83 응답.
 *
 * @param variables          템플릿 변수(순서대로)
 * @param subscriptionFilter 같은 범위의 MQTT 구독 필터
 * @param results            토픽마다 결과. 맞지 않으면 {@code matched=false}, {@code status=UNMATCHED_TOPIC}
 */
public record TopicTemplatePreviewResponse(List<String> variables, String subscriptionFilter, List<Result> results) {

    public record Result(String topic, boolean matched, Map<String, String> attributes, String status) {
    }
}
