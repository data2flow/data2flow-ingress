package net.java21.data2flow.ingress.payload.domain;

import net.java21.data2flow.contracts.connector.PayloadFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07·09.08 TC-DSC-280: 소스 설정 {@code payload}·{@code topicTemplate}(API-DSC-50 config) 읽기와 검증 */
class PayloadSettingsTest {

    static final JsonMapper JSON = JsonMapper.builder().build();

    static PayloadSettings of(String config) {
        return PayloadSettings.from(JSON.readTree(config), "mqtt");
    }

    @Test
    @DisplayName("DSC-09.07 설정이 없으면 JSON 그대로(통과), sparkplug-b 커넥터는 SPARKPLUG_B")
    void defaults() {
        PayloadSettings s = of("{}");
        assertThat(s.format()).isEqualTo(PayloadFormat.JSON);
        assertThat(s.passthrough()).isTrue();
        assertThat(s.converts()).isFalse();
        assertThat(PayloadSettings.from(JSON.readTree("{}"), "sparkplug-b").format()).isEqualTo(PayloadFormat.SPARKPLUG_B);
        assertThat(PayloadSettings.from(null, null).passthrough()).isTrue();
        assertThat(PayloadSettings.PASSTHROUGH.passthrough()).isTrue();
    }

    @Test
    @DisplayName("DSC-09.07 형식·압축·스키마·메시지 타입·레지스트리·CSV·템플릿을 읽는다(별칭 이름 포함)")
    void readsAll() {
        PayloadSettings s = of("""
                {"payload":{"format":"protobuf","compression":"gzip","schemaRef":" 42 ","messageType":"acme.Reading",
                 "registryUrl":"http://reg:8080/apis/ccompat/v7/","csv":{"delimiter":"tab","header":false}},
                 "topicTemplate":"site/{site}/{deviceId}"}""");
        assertThat(s.format()).isEqualTo(PayloadFormat.PROTOBUF);
        assertThat(s.compression()).isEqualTo(Compression.GZIP);
        assertThat(s.schemaRef()).isEqualTo("42");
        assertThat(s.messageType()).isEqualTo("acme.Reading");
        assertThat(s.registryUrl()).isEqualTo("http://reg:8080/apis/ccompat/v7");
        assertThat(s.csvDelimiter()).isEqualTo('\t');
        assertThat(s.csvHeader()).isFalse();
        assertThat(s.topicTemplate().variables()).containsExactly("site", "deviceId");
        assertThat(s.passthrough()).isFalse();
        assertThat(of("{\"payload\":{\"format\":\"MessagePack\"}}").format()).isEqualTo(PayloadFormat.MSGPACK);
        assertThat(of("{\"payload\":{\"format\":\"sparkplug-b\"}}").format()).isEqualTo(PayloadFormat.SPARKPLUG_B);
        assertThat(of("{\"payload\":{\"format\":\"SPARKPLUG\"}}").format()).isEqualTo(PayloadFormat.SPARKPLUG_B);
        assertThat(of("{\"payload\":{\"format\":\"proto\",\"schemaRef\":\"1\"}}").format()).isEqualTo(PayloadFormat.PROTOBUF);
        assertThat(of("{\"payload\":{\"format\":\"csv\",\"csv\":{\"delimiter\":\";\"}}}").csvDelimiter()).isEqualTo(';');
        assertThat(of("{\"payload\":{\"format\":\"text\",\"compression\":\"deflate\"}}").passthrough()).isFalse();
        assertThat(of("{\"payload\":{\"format\":\"binary\"}}").passthrough()).isTrue();
        assertThat(of("{\"payload\":{\"format\":\"avro\",\"schemaRef\":\"9\"}}").converts()).isTrue();
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "{\"payload\":{\"format\":\"xml\"}}|payload.format",
            "{\"payload\":{\"format\":\"json\",\"compression\":\"lz4\"}}|payload.compression",
            "{\"payload\":{\"format\":\"protobuf\"}}|payload.schemaRef",
            "{\"payload\":{\"format\":\"avro\"}}|payload.registryUrl",
            "{\"payload\":{\"format\":\"avro\",\"registryUrl\":\"ftp://x\"}}|payload.registryUrl",
            "{\"payload\":{\"format\":\"csv\",\"csv\":{\"delimiter\":\"ab\"}}}|payload.csv.delimiter",
            "{\"payload\":{\"format\":\"csv\",\"csv\":{\"delimiter\":\"\\\"\"}}}|payload.csv.delimiter",
            "{\"topicTemplate\":\"a/#/b\"}|topicTemplate"})
    @DisplayName("DSC-09.07 설정 오류는 필드 이름으로(SOURCE_CONFIG_INVALID)")
    void invalid(String config, String field) {
        assertThatThrownBy(() -> of(config)).isInstanceOf(IllegalArgumentException.class).hasMessage(field);
    }
}
