package net.java21.data2flow.ingress.source.dto;

import java.util.List;

/**
 * API-DSC-50 응답 전체(ACTIVE·PAUSED 소스 목록과 설정 버전). 변경이 있으면 core는 전체 목록을 준다.
 *
 * @param version 설정 버전. 다음 요청의 {@code sinceVersion}
 */
public record RuntimeConfigSnapshot(String version, List<SourceDefinition> sources) {

    public RuntimeConfigSnapshot {
        sources = sources == null ? List.of() : List.copyOf(sources);
    }
}
