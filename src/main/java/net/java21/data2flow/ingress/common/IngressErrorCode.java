package net.java21.data2flow.ingress.common;

import net.java21.data2flow.contracts.error.ErrorCode;

/**
 * ingress가 내는 DSC 오류 코드(spec/detail/00-error-codes.md). 공통 코드는
 * {@link net.java21.data2flow.contracts.error.CommonErrorCode}를 쓴다. 문구는 messages*.properties의 {@code error.<코드>}.
 */
public enum IngressErrorCode implements ErrorCode {
    /** 유형별 설정 위반(연결 테스트 요청) */
    SOURCE_CONFIG_INVALID(400),
    /** 커넥터가 지원하지 않는 인증 방식 */
    SOURCE_AUTH_UNSUPPORTED(400),
    /** 인증 방식에 필요한 비밀값 없음 */
    SOURCE_SECRET_REQUIRED(400),
    /** 올린 payload 스키마(.proto·.desc·.avsc)를 해석할 수 없음(API-DSC-82, DSC-09.07) */
    SOURCE_SCHEMA_INVALID(400);

    private final int httpStatus;

    IngressErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Override
    public String code() {
        return name();
    }

    @Override
    public int httpStatus() {
        return httpStatus;
    }
}
