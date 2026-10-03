package net.java21.data2flow.ingress.connectiontest;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.ingress.common.IngressErrorCode;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connectiontest.dto.SourceTestRequest;
import net.java21.data2flow.ingress.connectiontest.service.ConnectionTestService;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.support.FakeConnector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionTestServiceTest {

    static final JsonMapper JSON = JsonMapper.builder().build();
    final FakeConnector connector = new FakeConnector();
    final ConnectionTestService service = new ConnectionTestService(new ConnectorRegistry(List.of(connector)),
            new IngressProperties.ConnectionTest(Duration.ofSeconds(15), Duration.ofSeconds(30), 0));

    SourceTestRequest request(String type, String config) {
        return new SourceTestRequest(1L, type, null, JSON.readTree(config), null, null, 10);
    }

    @Test
    @DisplayName("API-DSC-51 동시 한도를 넘으면 429 RATE_LIMITED(Retry-After)")
    void rateLimited() {
        assertThatThrownBy(() -> service.test(request("MQTT", "{}")))
                .isInstanceOfSatisfying(BusinessException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(CommonErrorCode.RATE_LIMITED);
                    assertThat(e.getHeaders()).containsEntry("Retry-After", "5");
                });
    }

    @Test
    @DisplayName("BR-DSC-07 제한 시간: 없으면 15초, 30초 상한, 설정 오류 사유별 코드")
    void timeoutsAndErrors() {
        ConnectionTestService ok = new ConnectionTestService(new ConnectorRegistry(List.of(connector)),
                new IngressProperties.ConnectionTest(Duration.ofSeconds(15), Duration.ofSeconds(30), 3));
        assertThat(ok.test(new SourceTestRequest(1L, "MQTT_SUBSCRIBE", null, JSON.readTree("{}"),
                JSON.readTree("[{\"topic\":\"a\"}]"), JSON.readTree("[{\"kind\":\"PASSWORD\",\"value\":\"x\"}]"), 99))
                .succeeded()).isTrue();
        assertThat(ok.test(new SourceTestRequest(1L, "PLATFORM_BROKER", null, JSON.readTree("{}"), null,
                JSON.readTree("{\"PASSWORD\":\"x\"}"), null)).succeeded()).isTrue();
        assertThatThrownBy(() -> ok.test(request("MQTT", "{\"invalid\":\"SECRET_REQUIRED\"}")))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(IngressErrorCode.SOURCE_SECRET_REQUIRED));
        assertThatThrownBy(() -> ok.test(request("MQTT", "{\"invalid\":\"AUTH_UNSUPPORTED\"}")))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(IngressErrorCode.SOURCE_AUTH_UNSUPPORTED));
        assertThatThrownBy(() -> ok.test(request("MQTT", "{\"invalid\":\"CONFIG_INVALID\"}")))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(IngressErrorCode.SOURCE_CONFIG_INVALID));
        assertThatThrownBy(() -> ok.test(request("MQTT", "[1]")))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(IngressErrorCode.SOURCE_CONFIG_INVALID));
        assertThatThrownBy(() -> ok.test(request("WEBHOOK", "{}")))
                .isInstanceOfSatisfying(BusinessException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(IngressErrorCode.SOURCE_CONFIG_INVALID));
    }
}
