package net.java21.data2flow.ingress.connectiontest.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.ingress.connectiontest.dto.SourceTestRequest;
import net.java21.data2flow.ingress.connectiontest.service.ConnectionTestService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * API-DSC-51 {@code POST /internal/ingress/sources/test}: core-api(API-DSC-57 중계)가 부르는 연결 테스트. 권한(SRC_ADMIN)은 core가
 * 검사한다. 연결 실패는 HTTP 오류가 아니라 200 + 실패 단계로 돌려준다. 형식 오류는 400(SOURCE_CONFIG_INVALID 등).
 */
@RestController
public class InternalSourceTestController {

    private final ConnectionTestService service;

    public InternalSourceTestController(ConnectionTestService service) {
        this.service = service;
    }

    @PostMapping("/internal/ingress/sources/test")
    public ApiResponse<ConnectionTestResult> test(@Valid @RequestBody SourceTestRequest request) {
        return ApiResponse.success(service.test(request));
    }
}
