package net.java21.data2flow.ingress.connector.file;

import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07 TC-DSC-280: 파일 줄 → 구조화된 값(CSV 머리글·따옴표·숫자, jsonl 그대로) */
class LineFormatTest {

    @Test
    @DisplayName("DSC-09.07 CSV 두 겹 따옴표, 빈 칸, 정수·실수·문자열, 머리글보다 칸이 적으면 빈 문자열")
    void csv() {
        List<String> header = LineFormat.split("a,b,c,d");
        assertThat(new String(LineFormat.CSV.payload("\"say \"\"hi\"\"\",-3,1.5e2", header), StandardCharsets.UTF_8))
                .isEqualTo("{\"a\":\"say \\\"hi\\\"\",\"b\":-3,\"c\":150.0,\"d\":\"\"}");
        assertThat(LineFormat.CSV.hasHeader()).isTrue();
        assertThat(LineFormat.JSONL.hasHeader()).isFalse();
        assertThat(new String(LineFormat.JSONL.payload("{\"x\":1}", List.of()), StandardCharsets.UTF_8)).isEqualTo("{\"x\":1}");
        assertThat(LineFormat.of("csv")).isEqualTo(LineFormat.CSV);
        assertThatThrownBy(() -> LineFormat.of("parquet")).isInstanceOf(InvalidSettingsException.class);
    }
}
