package net.java21.data2flow.ingress.signing.service;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.source.event.ConfigChangedListener;
import net.java21.data2flow.ingress.source.service.RuntimeConfigSync;
import net.java21.data2flow.ingress.support.IngressFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** DSC-03.02(ADR-042): 서명 키 캐시 — API-DSC-72 읽기, 주기 60초 이하, 자격 변경 즉시 다시 읽기 */
class SigningKeyCacheTest {

    static final MessageCodec CODEC = MessageCodec.create();

    @Test
    @DisplayName("DSC-03.02 API-DSC-72를 X-CALLER-SERVICE로 읽고 문자열 ID·소문자 deviceKey·만료 시각을 읽는다(봉투·빈 값 처리)")
    void readsSigningKeys() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        IngressProperties props = IngressFixtures.props("prod", null, List.of(), List.of(), Map.of());
        SigningKeyClient client = new SigningKeyClient(builder, JsonMapper.builder().build(), props);
        server.expect(requestTo("http://core/internal/core/device-credentials/signing-keys"))
                .andExpect(method(HttpMethod.GET)).andExpect(header("X-CALLER-SERVICE", "data2flow-ingress"))
                .andRespond(withSuccess("""
                        {"header":{"isSuccessful":true,"resultCode":"SUCCESS"},"response":{"version":"4","keys":[
                          {"credentialId":"11","organizationId":"1","sourceId":"5","deviceId":"88","deviceKey":"ESP32-CO2-02",
                           "signingKey":"k1","expiresAt":"2027-01-01T00:00:00Z"},
                          {"credentialId":12,"sourceId":5,"deviceId":89,"deviceKey":"esp32-th-01","signingKey":"k2","expiresAt":"bad"},
                          {"sourceId":"5","deviceKey":"","signingKey":"k3"},
                          {"sourceId":"x","deviceKey":"a","signingKey":"k4"}
                        ]}}""", MediaType.APPLICATION_JSON));
        List<SigningKeyCache.Key> keys = client.fetch();
        assertThat(keys).hasSize(2);
        assertThat(keys.get(0).deviceKey()).isEqualTo("esp32-co2-02");
        assertThat(keys.get(0).credentialId()).isEqualTo(11);
        assertThat(keys.get(0).expiresAt()).isEqualTo(Instant.parse("2027-01-01T00:00:00Z"));
        assertThat(keys.get(1).expiresAt()).isNull();
        server.verify();
        assertThat(SigningKeyClient.parse(JsonMapper.builder().build().readTree("{\"keys\":[]}"))).isEmpty();
    }

    @Test
    @DisplayName("DSC-03.02 자격(CREDENTIAL) 설정 변경을 받으면 서명 키를 1초 안에 다시 읽고, 다른 종류는 키를 읽지 않는다")
    void refreshesOnCredentialChange() {
        AtomicInteger fetches = new AtomicInteger();
        try (SigningKeyCache cache = new SigningKeyCache(() -> {
            fetches.incrementAndGet();
            return List.of();
        }, Duration.ofSeconds(30), Clock.systemUTC())) {
            RuntimeConfigSync sync = new RuntimeConfigSync(null, null, null, null) {
                @Override
                public void requestRefresh() {
                }
            };
            ConfigChangedListener listener = new ConfigChangedListener(CODEC, sync, cache::requestRefresh);
            listener.onMessage(new Message(CODEC.write(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SOURCE, 3, 1, 1,
                    Clock.systemUTC())), new MessageProperties()));
            listener.onMessage(new Message(CODEC.write(ConfigChangedMessage.delete(ConfigChangedMessage.EntityType.CREDENTIAL, 9, 1, 1,
                    Clock.systemUTC())), new MessageProperties()));
            listener.onMessage(new Message(CODEC.write(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.CREDENTIAL, 10, 0, 1,
                    Clock.systemUTC())), new MessageProperties()));
            await().atMost(Duration.ofSeconds(3)).until(() -> fetches.get() == 1);
            cache.start();
            await().atMost(Duration.ofSeconds(3)).until(() -> fetches.get() == 2);
        }
    }

    @Test
    @DisplayName("DSC-03.02 서명 키 다시 읽기 주기는 60초를 넘을 수 없다(폐기 1분 안 반영)")
    void refreshIntervalBound() {
        assertThat(new IngressProperties.Signing(Duration.ofSeconds(60)).refreshInterval()).hasSeconds(60);
        assertThatThrownBy(() -> new IngressProperties.Signing(Duration.ofSeconds(61))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngressProperties.Signing(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngressProperties.Signing(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
