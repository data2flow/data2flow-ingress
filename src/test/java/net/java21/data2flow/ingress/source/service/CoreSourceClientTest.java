package net.java21.data2flow.ingress.source.service;

import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class CoreSourceClientTest {

    MockRestServiceServer server;
    CoreSourceClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        IngressProperties props = net.java21.data2flow.ingress.support.IngressFixtures.props("prod", null, List.of(), List.of(),
                Map.of("iot-data-basic", "iot-data:secret"));
        client = new CoreSourceClient(builder, JsonMapper.builder().build(), props);
    }

    @Test
    @DisplayName("DSC-01.01 API-DSC-50 실행 설정을 X-CALLER-SERVICE로 읽고, 유형 MQTT는 정본 MQTT_SUBSCRIBE로, topics·qos를 설정에 합친다")
    void readsRuntimeConfig() {
        server.expect(requestTo("http://core/internal/core/sources/runtime-config?lifecycle=ACTIVE,PAUSED"))
                .andExpect(method(HttpMethod.GET)).andExpect(header("X-CALLER-SERVICE", "data2flow-ingress"))
                .andRespond(withSuccess("""
                        {"version":"17","sources":[
                          {"id":3,"organizationId":1,"type":"MQTT","lifecycle":"ACTIVE",
                           "config":{"url":"wss://broker/mqtt","auth":"HEADER"},
                           "secrets":{"HEADER_VALUE":"u:p"},"topics":[{"topic":"application/+/device/+/event/up","qos":1}],
                           "qos":1,"clientId":"data2flow-chirpstack-s3"},
                          {"id":4,"organizationId":1,"type":"PLATFORM_BROKER","lifecycle":"PAUSED",
                           "connection":{"url":"wss://broker/mqtt"},"secrets":[{"kind":"PASSWORD","value":"pw"}],
                           "topics":[{"topic":"devices/+/telemetry","qos":1}]},
                          {"organizationId":1,"type":"MQTT"}
                        ]}""", MediaType.APPLICATION_JSON));
        RuntimeConfigSnapshot snap = client.fetch(null).orElseThrow();
        assertThat(snap.version()).isEqualTo("17");
        assertThat(snap.sources()).hasSize(2);
        SourceDefinition a = snap.sources().get(0);
        assertThat(a.type()).isEqualTo(SourceTypes.MQTT_SUBSCRIBE);
        assertThat(a.config().path("topics").get(0).path("topic").asString()).isEqualTo("application/+/device/+/event/up");
        assertThat(a.config().path("qos").asInt()).isEqualTo(1);
        assertThat(a.secrets().get("HEADER_VALUE").reveal()).isEqualTo("u:p");
        assertThat(a.clientIdBase()).isEqualTo("data2flow-chirpstack-s3");
        assertThat(a.toString()).doesNotContain("u:p");
        SourceDefinition b = snap.sources().get(1);
        assertThat(b.type()).isEqualTo(SourceTypes.PLATFORM_BROKER);
        assertThat(b.paused()).isTrue();
        assertThat(b.secrets().get("PASSWORD").reveal()).isEqualTo("pw");
        assertThat(b.config().path("url").asString()).isEqualTo("wss://broker/mqtt");
        server.verify();
    }

    @Test
    @DisplayName("DSC-01.01 sinceVersion이 같으면 core가 204 → 바뀐 것 없음")
    void notModified() {
        server.expect(requestTo("http://core/internal/core/sources/runtime-config?lifecycle=ACTIVE,PAUSED&sinceVersion=17"))
                .andRespond(withStatus(HttpStatus.NO_CONTENT));
        assertThat(client.fetch("17")).isEqualTo(Optional.empty());
    }

    @Test
    @DisplayName("공통 봉투({header, response})로 와도 읽고, credentialRef는 ingress 설정 비밀값(data2flow-ingress-mqtt)으로 채운다")
    void envelopeAndCredentialRef() {
        server.expect(requestTo("http://core/internal/core/sources/runtime-config?lifecycle=ACTIVE,PAUSED"))
                .andRespond(withSuccess("""
                        {"header":{"isSuccessful":true},"response":{"version":"2","sources":[
                          {"id":3,"organizationId":1,"type":"MQTT_SUBSCRIBE","lifecycle":"ACTIVE",
                           "config":{"connector":"mqtt","url":"wss://iot-data.java21.net:443/mqtt",
                                     "clientIdPrefix":"data2flow-ingress",
                                     "subscriptions":[{"topic":"application/+/device/+/event/up","qos":1}],
                                     "auth":{"type":"ws-header","scheme":"Basic","credentialRef":"secret://sources/iot-data/basic"}}}
                        ]}}""", MediaType.APPLICATION_JSON));
        SourceDefinition d = client.fetch(null).orElseThrow().sources().getFirst();
        assertThat(d.secrets().get("HEADER_VALUE").reveal()).isEqualTo("iot-data:secret");
        assertThat(d.connectorKey()).isEqualTo("mqtt");
        assertThat(d.clientIdBase()).isEqualTo("data2flow-ingress");
    }

    static final String PLATFORM_ONLY = """
            {"version":"1","sources":[{"id":5,"organizationId":1,"type":"PLATFORM_BROKER","lifecycle":"ACTIVE",
              "config":{"deviceKeyPattern":"{externalId}"}}]}""";

    SourceDefinition platformSource(IngressProperties.PlatformBroker broker, Map<String, String> credentials) {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer s = MockRestServiceServer.bindTo(builder).build();
        IngressProperties props = net.java21.data2flow.ingress.support.IngressFixtures.props("prod", null, List.of(), List.of(),
                credentials, broker);
        s.expect(requestTo("http://core/internal/core/sources/runtime-config?lifecycle=ACTIVE,PAUSED"))
                .andRespond(withSuccess(PLATFORM_ONLY, MediaType.APPLICATION_JSON));
        return new CoreSourceClient(builder, JsonMapper.builder().build(), props).fetch(null).orElseThrow().sources().get(0);
    }

    @Test
    @DisplayName("DSC-03.01 주소 없는 플랫폼 브로커 소스는 ingress 설정의 공용 브로커 주소·토픽·버전과 MQTT_BASIC_AUTH Basic 헤더를 쓴다")
    void platformBrokerDefaultsToIotData() {
        SourceDefinition d = platformSource(new IngressProperties.PlatformBroker("wss://iot-data.java21.net/mqtt",
                List.of("devices/+/telemetry"), "5.0", null, null), Map.of("iot-data-basic", "iot-data:secret"));
        assertThat(d.config().path("url").asString()).isEqualTo("wss://iot-data.java21.net/mqtt");
        assertThat(d.config().path("topics").get(0).path("topic").asString()).isEqualTo("devices/+/telemetry");
        assertThat(d.config().path("topics").get(0).path("qos").asInt()).isEqualTo(1);
        assertThat(d.config().path("version").asString()).isEqualTo("5.0");
        assertThat(d.config().path("auth").asString()).isEqualTo("HEADER");
        assertThat(d.secrets().get("HEADER_VALUE").reveal()).isEqualTo("iot-data:secret");
        assertThat(d.config().path("deviceKeyPattern").asString()).isEqualTo("{externalId}");
    }

    @Test
    @DisplayName("DSC-03.01 플랫폼 브로커 주소가 다른 tcp 브로커면 사용자/비밀번호로, 접속 정보가 없으면 인증 없이")
    void platformBrokerLocal() {
        SourceDefinition withUser = platformSource(new IngressProperties.PlatformBroker("tcp://localhost:1883",
                List.of("devices/+/telemetry", " devices/+/state "), "3.1.1", "dev", "pw"), Map.of("iot-data-basic", "x:y"));
        assertThat(withUser.config().path("auth").asString()).isEqualTo("USERPASS");
        assertThat(withUser.config().path("username").asString()).isEqualTo("dev");
        assertThat(withUser.secrets().get("PASSWORD").reveal()).isEqualTo("pw");
        assertThat(withUser.config().path("topics").get(1).path("topic").asString()).isEqualTo("devices/+/state");
        SourceDefinition wsUser = platformSource(new IngressProperties.PlatformBroker("ws://localhost:9001/mqtt",
                List.of("devices/+/telemetry"), "5.0", "dev", "pw"), Map.of());
        assertThat(wsUser.config().path("auth").asString()).isEqualTo("HEADER");
        assertThat(wsUser.secrets().get("HEADER_VALUE").reveal()).isEqualTo("dev:pw");
        SourceDefinition none = platformSource(new IngressProperties.PlatformBroker("tcp://localhost:1883",
                List.of("devices/+/telemetry"), "5.0", null, null), Map.of("iot-data-basic", "x:y"));
        assertThat(none.config().has("auth")).isFalse();
        assertThat(none.secrets()).isEmpty();
        SourceDefinition bad = platformSource(new IngressProperties.PlatformBroker("not a url",
                List.of("devices/+/telemetry"), "5.0", null, null), Map.of());
        assertThat(bad.config().path("url").asString()).isEqualTo("not a url");
    }
}
