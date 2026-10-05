package net.java21.data2flow.ingress.payload.codec;

import net.java21.data2flow.ingress.payload.PayloadTestData.SpMetric;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static net.java21.data2flow.ingress.payload.PayloadTestData.sparkplug;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07 AT-DSC-16.3 TC-DSC-284: Sparkplug B BIRTH 별칭으로 DATA 값을 측정 항목 이름에 매핑 */
class SparkplugBCodecTest {

    @Test
    @DisplayName("DSC-09.07 AT-DSC-16.3 NBIRTH·DBIRTH의 별칭으로 DDATA 값의 이름·자료형을 채운다")
    void birthAliasesNameData() throws Exception {
        SparkplugBCodec codec = new SparkplugBCodec();
        JsonNode nbirth = codec.decode(sparkplug(1000, 0, new SpMetric("Node Control/Rebirth", 1L, 11, false)),
                "spBv1.0/plant/NBIRTH/edge-1");
        assertThat(nbirth.get("messageType").asString()).isEqualTo("NBIRTH");
        assertThat(nbirth.get("metrics").get(0).get("value").asBoolean()).isFalse();
        codec.decode(sparkplug(1001, 1, new SpMetric("temperature", 10L, 10, 0.0),
                new SpMetric("count", 11L, 3, 0)), "spBv1.0/plant/DBIRTH/edge-1/em-1");
        JsonNode ddata = codec.decode(sparkplug(1002, 2, new SpMetric(null, 10L, null, 22.5),
                new SpMetric(null, 11L, null, -5)), "spBv1.0/plant/DDATA/edge-1/em-1");
        assertThat(ddata.get("deviceId").asString()).isEqualTo("em-1");
        assertThat(ddata.get("edgeNodeId").asString()).isEqualTo("edge-1");
        assertThat(ddata.get("groupId").asString()).isEqualTo("plant");
        assertThat(ddata.get("timestamp").asLong()).isEqualTo(1002);
        assertThat(ddata.get("seq").asLong()).isEqualTo(2);
        JsonNode t = ddata.get("metrics").get(0);
        assertThat(t.get("name").asString()).isEqualTo("temperature");
        assertThat(t.get("alias").asLong()).isEqualTo(10);
        assertThat(t.get("datatype").asString()).isEqualTo("Double");
        assertThat(t.get("value").asDouble()).isEqualTo(22.5);
        assertThat(ddata.get("metrics").get(1).get("value").asInt()).as("Int32는 2의 보수").isEqualTo(-5);
        assertThat(codec.knownNodes()).isEqualTo(1);

        // NBIRTH가 다시 오면 별칭을 비운다 → 모르는 별칭은 이름 없이 alias만
        codec.decode(sparkplug(2000, 0), "spBv1.0/plant/NBIRTH/edge-1");
        JsonNode after = codec.decode(sparkplug(2001, 1, new SpMetric(null, 10L, null, 1.0)),
                "spBv1.0/plant/DDATA/edge-1/em-1");
        assertThat(after.get("metrics").get(0).get("name").isNull()).isTrue();
        assertThat(after.get("metrics").get(0).get("alias").asLong()).isEqualTo(10);
    }

    @Test
    @DisplayName("DSC-09.07 자료형: Int8·Int16·UInt32·Int64·UInt64·Float·String·Bytes·null·DataSet·모르는 자료형")
    void datatypes() throws Exception {
        JsonNode out = new SparkplugBCodec().decode(sparkplug(1, 1,
                new SpMetric("i8", null, 1, 0xFF), new SpMetric("i16", null, 2, 0xFFFF), new SpMetric("u32", null, 7, -1),
                new SpMetric("i64", null, 4, -2L), new SpMetric("u64", null, 8, -1L), new SpMetric("f", null, 9, 1.5f),
                new SpMetric("s", null, 12, "on"), new SpMetric("b", null, 17, new byte[]{1}),
                new SpMetric("n", null, 10, null), new SpMetric("x", null, 99, true)), "spBv1.0/g/NDATA/e");
        JsonNode m = out.get("metrics");
        assertThat(m.get(0).get("value").asInt()).isEqualTo(-1);
        assertThat(m.get(1).get("value").asInt()).isEqualTo(-1);
        assertThat(m.get(2).get("value").asLong()).isEqualTo(4294967295L);
        assertThat(m.get(3).get("value").asLong()).isEqualTo(-2);
        assertThat(m.get(4).get("value").bigIntegerValue().toString()).isEqualTo("18446744073709551615");
        assertThat(m.get(5).get("value").asDouble()).isEqualTo(1.5);
        assertThat(m.get(6).get("value").asString()).isEqualTo("on");
        assertThat(m.get(7).get("value").asString()).isEqualTo("AQ==");
        assertThat(m.get(8).get("isNull").asBoolean()).isTrue();
        assertThat(m.get(8).get("value").isNull()).isTrue();
        assertThat(m.get(9).get("datatype").asString()).isEqualTo("99");
        assertThat(out.has("deviceId")).isFalse();
    }

    @Test
    @DisplayName("DSC-09.07 DataSet·Template은 unsupported, 이력·일시 표시, uuid·body, 깨진 payload·Sparkplug 아닌 토픽은 DECODE_ERROR")
    void flagsAndErrors() throws Exception {
        java.io.ByteArrayOutputStream metric = new java.io.ByteArrayOutputStream();
        com.google.protobuf.CodedOutputStream mc = com.google.protobuf.CodedOutputStream.newInstance(metric);
        mc.writeString(1, "ds");
        mc.writeUInt64(3, 5);
        mc.writeUInt32(4, 16);
        mc.writeBool(5, true);
        mc.writeBool(6, true);
        mc.writeByteArray(17, new byte[]{8, 1});
        mc.writeString(99, "ignored");
        mc.flush();
        java.io.ByteArrayOutputStream payload = new java.io.ByteArrayOutputStream();
        com.google.protobuf.CodedOutputStream c = com.google.protobuf.CodedOutputStream.newInstance(payload);
        c.writeByteArray(2, metric.toByteArray());
        c.writeString(4, "uuid-1");
        c.writeByteArray(5, new byte[]{7});
        c.writeString(9, "skip");
        c.flush();
        JsonNode out = new SparkplugBCodec().decode(payload.toByteArray(), "spBv1.0/g/DDATA/e/d");
        JsonNode m = out.get("metrics").get(0);
        assertThat(m.get("unsupported").asBoolean()).isTrue();
        assertThat(m.get("isHistorical").asBoolean()).isTrue();
        assertThat(m.get("isTransient").asBoolean()).isTrue();
        assertThat(m.get("timestamp").asLong()).isEqualTo(5);
        assertThat(m.get("datatype").asString()).isEqualTo("DataSet");
        assertThat(out.get("uuid").asString()).isEqualTo("uuid-1");
        assertThat(out.get("body").asString()).isEqualTo("Bw==");
        assertThatThrownBy(() -> new SparkplugBCodec().decode(new byte[]{0x12, 0x05, 0x01}, "spBv1.0/g/NDATA/e"))
                .isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> new SparkplugBCodec().decode(new byte[0], "a/b")).hasMessageContaining("토픽");
        assertThatThrownBy(() -> new SparkplugBCodec().decode(new byte[0], null)).hasMessageContaining("토픽");
    }
}
