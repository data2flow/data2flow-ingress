package net.java21.data2flow.ingress.connector.mqtt;

import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttSourceSettingsTest {

    static final JsonMapper JSON = JsonMapper.builder().build();

    static MqttSourceSettings parse(String config, Map<String, Secret> secrets) {
        return MqttSourceSettings.from(new SourceConfig(1, 3, SourceTypes.MQTT_SUBSCRIBE, "mqtt", JSON.readTree(config),
                secrets, "data2flow-ingress-prod-0"));
    }

    static MqttSourceSettings parse(String config) {
        return parse(config, Map.of());
    }

    @Test
    @DisplayName("DSC-01.04 TC-DSC-025 아카데미 iot-data 설정(connectors.md §3 모양)을 읽는다: wss·경로·Basic 헤더·영속 세션·QoS 1")
    void academyIotDataConfig() {
        MqttSourceSettings s = parse("""
                {"connector":"mqtt","version":"5.0","url":"wss://iot-data.java21.net:443/mqtt",
                 "subscriptions":[{"topic":"application/+/device/+/event/up","qos":1,"shared":false}],
                 "session":{"cleanStart":false,"sessionExpirySec":3600,"keepAliveSec":30,"receiveMaximum":100},
                 "auth":{"type":"ws-header","scheme":"Basic","credentialRef":"secret://sources/iot-data/basic"},
                 "tls":{"verify":true,"minVersion":"TLSv1.2"}}
                """, Map.of("HEADER_VALUE", Secret.of("iot-data:pw")));
        assertThat(s.transport()).isEqualTo("wss");
        assertThat(s.host()).isEqualTo("iot-data.java21.net");
        assertThat(s.port()).isEqualTo(443);
        assertThat(s.path()).isEqualTo("mqtt");
        assertThat(s.webSocket()).isTrue();
        assertThat(s.tls()).isTrue();
        assertThat(s.keepAliveSec()).isEqualTo(30);
        assertThat(s.cleanStart()).isFalse();
        assertThat(s.sessionExpirySec()).isEqualTo(3600);
        assertThat(s.topics()).containsExactly(new MqttSourceSettings.Subscription("application/+/device/+/event/up", 1));
        assertThat(s.headers()).containsEntry("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("iot-data:pw".getBytes(StandardCharsets.UTF_8)));
        assertThat(s.tlsInsecure()).isFalse();
        assertThat(s.scaling()).isEqualTo(ScalingMode.DUAL_ACTIVE);
        assertThat(s.lossPossible()).isFalse();
        assertThat(s.toString()).doesNotContain("pw");
    }

    @Test
    @DisplayName("DSC-01.04 도메인 모델 §2.2 평평한 모양과 기본값(keepalive 60, 영속 세션, 만료 3600, 버전 5.0, 포트)")
    void flatShapeAndDefaults() {
        MqttSourceSettings s = parse("{\"url\":\"tcp://broker.local\",\"topics\":[{\"topic\":\"a/#\"}]}");
        assertThat(s.version()).isEqualTo(MqttSourceSettings.V5);
        assertThat(s.port()).isEqualTo(1883);
        assertThat(s.keepAliveSec()).isEqualTo(60);
        assertThat(s.cleanStart()).isFalse();
        assertThat(s.sessionExpirySec()).isEqualTo(3600);
        assertThat(s.topics().getFirst().qos()).isEqualTo(1);
        assertThat(s.auth()).isEqualTo("NONE");
        assertThat(parse("{\"url\":\"ssl://b\",\"topics\":[\"x\"]}").port()).isEqualTo(8883);
        assertThat(parse("{\"url\":\"ws://b\",\"topics\":[\"x\"]}").port()).isEqualTo(80);
        assertThat(parse("{\"url\":\"wss://b\",\"topics\":[\"x\"]}").port()).isEqualTo(443);
        assertThat(parse("{\"url\":\"mqtt://b:1999\",\"topics\":[\"x\"],\"version\":\"3.1.1\"}").version())
                .isEqualTo(MqttSourceSettings.V311);
    }

    @Test
    @DisplayName("DSC-09.04 BR-DSC-25 TC-DSC-263 MQTT 5 + 공유 구독이면 SCALABLE($share/{group}/토픽), 3.1.1에 공유 구독은 거부")
    void sharedSubscription() {
        MqttSourceSettings s = parse("{\"url\":\"tcp://b\",\"topics\":[{\"topic\":\"a/+\",\"qos\":1}],\"sharedGroup\":\"ingress\"}");
        assertThat(s.scaling()).isEqualTo(ScalingMode.SCALABLE);
        assertThat(s.topics().getFirst().topic()).isEqualTo("$share/ingress/a/+");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"version\":\"3.1.1\",\"topics\":[\"a\"],\"sharedGroup\":\"g\"}"))
                .isInstanceOf(InvalidSettingsException.class).hasMessageContaining("sharedGroup");
    }

    @Test
    @DisplayName("DSC-09.03 AT-DSC-20.4 QoS 0 토픽이나 clean start는 유실 가능으로 표시한다")
    void lossPossible() {
        assertThat(parse("{\"url\":\"tcp://b\",\"topics\":[{\"topic\":\"a\",\"qos\":0}]}").lossPossible()).isTrue();
        assertThat(parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"cleanStart\":true}").lossPossible()).isTrue();
    }

    @Test
    @DisplayName("DSC-01.05 TC-DSC-032 인증 방식별 비밀값: 사용자/비밀번호, HTTP 헤더(ws·wss만), mTLS. 없으면 SOURCE_SECRET_REQUIRED")
    void authMethods() {
        MqttSourceSettings up = parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"auth\":\"USERPASS\",\"username\":\"u\"}",
                Map.of("PASSWORD", Secret.of("p")));
        assertThat(up.username()).isEqualTo("u");
        assertThat(up.password().reveal()).isEqualTo("p");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"auth\":\"USERPASS\",\"username\":\"u\"}"))
                .isInstanceOfSatisfying(InvalidSettingsException.class,
                        e -> assertThat(e.reason()).isEqualTo(InvalidSettingsException.Reason.SECRET_REQUIRED));
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"auth\":\"USERPASS\"}",
                Map.of("password", Secret.of("p")))).isInstanceOf(InvalidSettingsException.class).hasMessageContaining("username");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"auth\":\"HEADER\"}",
                Map.of("HEADER_VALUE", Secret.of("x"))))
                .isInstanceOfSatisfying(InvalidSettingsException.class,
                        e -> assertThat(e.reason()).isEqualTo(InvalidSettingsException.Reason.AUTH_UNSUPPORTED));
        MqttSourceSettings header = parse("{\"url\":\"ws://b/m\",\"topics\":[\"a\"],\"auth\":\"HEADER\",\"headerName\":\"X-Key\",\"headerScheme\":\"Bearer\"}",
                Map.of("HEADER_VALUE", Secret.of("tok")));
        assertThat(header.headers()).containsEntry("X-Key", "Bearer tok");
        assertThat(header.path()).isEqualTo("m");
        assertThatThrownBy(() -> parse("{\"url\":\"ssl://b\",\"topics\":[\"a\"],\"auth\":\"MTLS\"}"))
                .isInstanceOf(InvalidSettingsException.class);
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"auth\":{\"type\":\"kerberos\"}}"))
                .isInstanceOfSatisfying(InvalidSettingsException.class,
                        e -> assertThat(e.reason()).isEqualTo(InvalidSettingsException.Reason.AUTH_UNSUPPORTED));
    }

    @Test
    @DisplayName("DSC-01.05 Basic 헤더 값: 사용자:비밀번호면 Base64, 이미 'Basic …'이면 그대로")
    void basicHeaderValue() {
        assertThat(MqttSourceSettings.headerValue("Basic", "Authorization", "Basic abc")).isEqualTo("Basic abc");
        assertThat(MqttSourceSettings.headerValue(null, "Authorization", "u:p")).isEqualTo("Basic dTpw");
        assertThat(MqttSourceSettings.headerValue(null, "X-Api-Key", "raw")).isEqualTo("raw");
    }

    @Test
    @DisplayName("DSC-07.03 BR-DSC-06 SOURCE_CONFIG_INVALID: url 없음·스킴 틀림·토픽 없음·토픽 21개·QoS 3·keepalive 범위 밖")
    void invalidConfigs() {
        assertThatThrownBy(() -> parse("{\"topics\":[\"a\"]}")).hasMessageContaining("url");
        assertThatThrownBy(() -> parse("{\"url\":\"http://b\",\"topics\":[\"a\"]}")).hasMessageContaining("url");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://\",\"topics\":[\"a\"]}")).hasMessageContaining("url");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\"}")).hasMessageContaining("topics");
        StringBuilder many = new StringBuilder("[");
        for (int i = 0; i < 21; i++) {
            many.append(i == 0 ? "" : ",").append("\"t").append(i).append('"');
        }
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":" + many + "]}")).hasMessageContaining("topics");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[{\"topic\":\"a\",\"qos\":3}]}")).hasMessageContaining("qos");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"keepaliveSec\":5}")).hasMessageContaining("keepaliveSec");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"keepaliveSec\":\"x\"}")).hasMessageContaining("keepaliveSec");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[\"a\"],\"version\":\"4\"}")).hasMessageContaining("version");
        assertThatThrownBy(() -> parse("{\"url\":\"tcp://b\",\"topics\":[{\"qos\":1}]}")).hasMessageContaining("topics");
    }

    @Test
    @DisplayName("BR-DSC-29 tls.verify=false·tlsInsecure는 검증 끄기로 읽는다")
    void tlsInsecure() {
        assertThat(parse("{\"url\":\"ssl://b\",\"topics\":[\"a\"],\"tls\":{\"verify\":false}}").tlsInsecure()).isTrue();
        assertThat(parse("{\"url\":\"ssl://b\",\"topics\":[\"a\"],\"tlsInsecure\":true}").tlsInsecure()).isTrue();
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] downlinkAck=true면 ChirpStack 업링크 토픽마다 event/ack·event/txack을 QoS 1 이상으로 더 구독한다(공유 구독 접두사 포함)")
    void downlinkAckAddsChirpStackAckTopics() {
        MqttSourceSettings s = parse("""
                {"url":"tcp://localhost:1883","topics":[{"topic":"application/app-1/device/+/event/up","qos":0},
                 {"topic":"devices/+/telemetry","qos":1}],"downlinkAck":true}""");
        assertThat(s.topics()).containsExactly(
                new MqttSourceSettings.Subscription("application/app-1/device/+/event/up", 0),
                new MqttSourceSettings.Subscription("devices/+/telemetry", 1),
                new MqttSourceSettings.Subscription("application/app-1/device/+/event/ack", 1),
                new MqttSourceSettings.Subscription("application/app-1/device/+/event/txack", 1));

        MqttSourceSettings shared = parse("""
                {"url":"tcp://localhost:1883","sharedGroup":"g1","downlinkAck":true,
                 "topics":[{"topic":"application/+/device/+/event/up","qos":1},{"topic":"application/+/device/+/event/ack","qos":2}]}""");
        assertThat(shared.topics()).extracting(MqttSourceSettings.Subscription::topic).containsExactly(
                "$share/g1/application/+/device/+/event/up", "$share/g1/application/+/device/+/event/ack",
                "$share/g1/application/+/device/+/event/txack");

        assertThat(parse("""
                {"url":"tcp://localhost:1883","topics":["application/+/device/+/event/up"]}""").topics()).hasSize(1);
        assertThatThrownBy(() -> parse("""
                {"url":"tcp://localhost:1883","topics":["devices/+/telemetry"],"downlinkAck":true}"""))
                .isInstanceOf(InvalidSettingsException.class).hasMessageContaining("downlinkAck");
    }
}
