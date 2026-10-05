package net.java21.data2flow.ingress.payload.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * API-DSC-83 요청: 토픽 템플릿 미리보기(UC-DSC-16 3, UI-DSC-08 [페이로드] 탭).
 *
 * @param template 템플릿(256자 이하)
 * @param topics   맞춰 볼 토픽·경로(최대 20개)
 */
public record TopicTemplatePreviewRequest(@NotBlank @Size(max = 256) String template,
                                          @NotNull @Size(max = 20) List<@NotNull @Size(max = 512) String> topics) {
}
