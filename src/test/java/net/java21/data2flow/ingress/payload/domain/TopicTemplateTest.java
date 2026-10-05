package net.java21.data2flow.ingress.payload.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.08 TC-DSC-289 AT-DSC-16.4·16.5 BR-DSC-28: 토픽 템플릿 추출·불일치·'+'·'#' 이스케이프 */
class TopicTemplateTest {

    static final String SITE = "site/{site}/room/{room}/{deviceId}/{metric}";

    @Test
    @DisplayName("DSC-09.08 AT-DSC-16.4 site/a/room/301/em-1/temperature → externalId em-1, 측정 항목 temperature, 공간 힌트 a/301")
    void extractsDeviceMetricSpace() {
        Map<String, String> m = TopicTemplate.compile(SITE).match("site/a/room/301/em-1/temperature").orElseThrow();
        assertThat(m).containsEntry("externalId", "em-1").containsEntry("metric", "temperature")
                .containsEntry("spaceHint", "a/301").containsEntry("site", "a").containsEntry("room", "301")
                .containsEntry("deviceId", "em-1");
    }

    @Test
    @DisplayName("DSC-09.08 AT-DSC-16.5 같은 템플릿에 site/a/em-1은 맞지 않는다(UNMATCHED_TOPIC)")
    void unmatched() {
        assertThat(TopicTemplate.compile(SITE).match("site/a/em-1")).isEmpty();
        assertThat(TopicTemplate.compile(SITE).match(null)).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] {0} ← {1}")
    @CsvSource(delimiter = '|', nullValues = "NULL", value = {
            // 템플릿 | 토픽 | 기대(k=v;… 또는 NO)
            "site/{site}/room/{room}/{deviceId}/{metric}|site/b/room/1/x/co2|site=b;room=1;deviceId=x;metric=co2;externalId=x;spaceHint=b/1",
            "site/{site}/room/{room}/{deviceId}/{metric}|site/b/room/1/x|NO",
            "site/{site}/room/{room}/{deviceId}/{metric}|site/b/room/1/x/co2/extra|NO",
            "application/{appId}/device/{externalId}/event/up|application/7/device/24e1/event/up|appId=7;externalId=24e1",
            "application/{appId}/device/{externalId}/event/up|application/7/device/24e1/event/join|NO",
            "devices/+/telemetry/{metric}|devices/abc/telemetry/temp|metric=temp",
            "devices/+/telemetry/{metric}|devices/a/b/telemetry/temp|NO",
            "spBv1.0/{group}/+/{edge}/#|spBv1.0/g/DDATA/e/d|group=g;edge=e",
            "spBv1.0/{group}/+/{edge}/#|spBv1.0/g/NDATA/e|group=g;edge=e",
            "#|anything/at/all|",
            "dev-{deviceKey}/data|dev-abc-1/data|deviceKey=abc-1;externalId=abc-1",
            "dev-{deviceKey}/data|abc/data|NO",
            "{building}/{floor#}|hq/3/east|building=hq;floor=3/east;spaceHint=hq/3/east",
            "a+b/{devEui}|a+b/0011|devEui=0011;externalId=0011",
            "a+b/{devEui}|aab/0011|NO",
            "a\\+/\\#x/{device}|a+/#x/d1|device=d1;externalId=d1",
            "a.b/(x)/{zone}|a.b/(x)/z1|zone=z1;spaceHint=z1",
            "a.b/(x)/{zone}|aXb/(x)/z1|NO",
            "/ingest/webhook/{source}/{area}|/ingest/webhook/hvac/lobby|source=hvac;area=lobby;spaceHint=lobby",
            "lit\\{x\\}/{space}|lit{x}/s|space=s;spaceHint=s"})
    @DisplayName("DSC-09.08 TC-DSC-289 매개변수화 20케이스: 변수·와이어드카드·부분 단계·정규식 특수문자·이스케이프")
    void cases(String template, String topic, String expected) {
        Optional<Map<String, String>> m = TopicTemplate.compile(template).match(topic);
        if ("NO".equals(expected)) {
            assertThat(m).isEmpty();
            return;
        }
        assertThat(m).isPresent();
        if (expected == null || expected.isEmpty()) {
            assertThat(m.get()).isEmpty();
            return;
        }
        for (String kv : expected.split(";")) {
            String[] p = kv.split("=", 2);
            assertThat(m.get()).containsEntry(p[0], p[1]);
        }
    }

    @Test
    @DisplayName("DSC-09.08 MQTT 구독 필터와 변수 목록(미리보기)")
    void filterAndVariables() {
        TopicTemplate t = TopicTemplate.compile(SITE);
        assertThat(t.subscriptionFilter()).isEqualTo("site/+/room/+/+/+");
        assertThat(t.variables()).containsExactly("site", "room", "deviceId", "metric");
        assertThat(TopicTemplate.compile("a/{x#}").subscriptionFilter()).isEqualTo("a/#");
        assertThat(TopicTemplate.compile("a\\+/b").subscriptionFilter()).isEqualTo("a+/b");
        assertThat(TopicTemplate.compile(" a/{x} ").toString()).isEqualTo("a/{x}");
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {"a/#/b|마지막", "a/{x#}/b|마지막", "a/{x|닫히지", "a/x}|짝", "a/{1x}|이름",
            "{a}/{a}|두 번", "a/{x}{y}|붙여"})
    @DisplayName("DSC-09.08 템플릿 문법 오류는 SOURCE_CONFIG_INVALID(topicTemplate)")
    void invalid(String template, String reason) {
        assertThatThrownBy(() -> TopicTemplate.compile(template)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(reason);
    }

    @Test
    @DisplayName("DSC-09.08 빈 템플릿·256자 초과는 거부")
    void blankAndTooLong() {
        assertThatThrownBy(() -> TopicTemplate.compile(" ")).hasMessageContaining("비어");
        assertThatThrownBy(() -> TopicTemplate.compile(null)).hasMessageContaining("비어");
        assertThatThrownBy(() -> TopicTemplate.compile("a".repeat(257))).hasMessageContaining("256");
    }
}
