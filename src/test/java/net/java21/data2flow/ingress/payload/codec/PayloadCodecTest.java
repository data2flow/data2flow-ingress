package net.java21.data2flow.ingress.payload.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import net.java21.data2flow.ingress.payload.PayloadTestData;
import net.java21.data2flow.ingress.payload.domain.Compression;
import net.java21.data2flow.ingress.payload.schema.ProtoSchemaParser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import tools.jackson.databind.JsonNode;
import tools.jackson.dataformat.cbor.CBORMapper;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07 TC-DSC-280: 형식별 변환 규칙과 DECODE_ERROR(형식과 실제 payload가 맞지 않음, UC-DSC-16 1a) */
class PayloadCodecTest {

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("DSC-09.07 Protobuf: 0 값도 싣고, 부호 없는 정수·bytes·enum·map·Timestamp·래퍼·repeated·모르는 필드 번호")
    void protobufRules() throws Exception {
        Descriptor d = ProtoSchemaParser.find(ProtoSchemaParser.parse("m.proto", utf8("""
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                import "google/protobuf/wrappers.proto";
                enum Mode { MODE_UNSPECIFIED = 0; AUTO = 1; }
                message M {
                  double zero = 1; uint32 u32 = 2; uint64 u64 = 3; bytes raw = 4; Mode mode = 5;
                  map<string, int32> tags = 6; google.protobuf.Timestamp at = 7; google.protobuf.DoubleValue w = 8;
                  repeated int64 list = 9; optional int32 opt = 10; Sub sub = 11; fixed32 f32 = 12;
                }
                message Sub { string s = 1; }
                """)), "M");
        Descriptor entry = d.findFieldByName("tags").getMessageType();
        DynamicMessage msg = DynamicMessage.newBuilder(d)
                .setField(d.findFieldByName("u32"), -1)
                .setField(d.findFieldByName("u64"), -1L)
                .setField(d.findFieldByName("raw"), ByteString.copyFrom(new byte[]{1, 2}))
                .setField(d.findFieldByName("mode"), d.getFile().findEnumTypeByName("Mode").findValueByNumber(1))
                .addRepeatedField(d.findFieldByName("tags"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "k").setField(entry.findFieldByName("value"), 3).build())
                .setField(d.findFieldByName("at"), Timestamp.newBuilder().setSeconds(1_780_000_000L).setNanos(5_000_000).build())
                .setField(d.findFieldByName("w"), com.google.protobuf.DoubleValue.of(1.5))
                .addRepeatedField(d.findFieldByName("list"), 7L)
                .setField(d.findFieldByName("f32"), -2)
                .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())
                .build();
        JsonNode out = new ProtobufCodec(d).decode(msg.toByteArray(), null);
        assertThat(out.get("zero").asDouble()).as("0도 싣는다").isZero();
        assertThat(out.get("u32").asLong()).isEqualTo(4294967295L);
        assertThat(out.get("u64").bigIntegerValue().toString()).isEqualTo("18446744073709551615");
        assertThat(out.get("raw").asString()).isEqualTo("AQI=");
        assertThat(out.get("mode").asString()).isEqualTo("AUTO");
        assertThat(out.get("tags").get("k").asInt()).isEqualTo(3);
        assertThat(out.get("at").asString()).isEqualTo(Instant.ofEpochSecond(1_780_000_000L, 5_000_000).toString());
        assertThat(out.get("w").asDouble()).isEqualTo(1.5);
        assertThat(out.get("list").get(0).asLong()).isEqualTo(7);
        assertThat(out.has("opt")).as("optional은 있을 때만").isFalse();
        assertThat(out.has("sub")).isFalse();
        assertThat(out.get("f32").asLong()).isEqualTo(4294967294L);
        assertThat(out.get("_unknownFields").get(0).asInt()).isEqualTo(99);
        assertThatThrownBy(() -> new ProtobufCodec(d).decode(new byte[]{(byte) 0xFF, (byte) 0xFF}, null))
                .isInstanceOf(PayloadDecodeException.class).hasMessageContaining("M");
        Descriptor req = ProtoSchemaParser.find(ProtoSchemaParser.parse("r.proto", utf8("message R { required int32 a = 1; }")),
                "R");
        assertThatThrownBy(() -> new ProtobufCodec(req).decode(new byte[0], null)).hasMessageContaining("required fields");
        assertThat(new ProtobufCodec(req).type()).isEqualTo(req);
    }

    @Test
    @DisplayName("DSC-09.07 CBOR: 바이트 문자열은 base64, 남는 값·깨진 입력은 DECODE_ERROR")
    void cborRules() throws Exception {
        CBORMapper cbor = new CBORMapper();
        byte[] withBytes = cbor.writeValueAsBytes(Map.of("b", new byte[]{1, 2}, "a", List.of(new byte[]{3})));
        JsonNode out = new CborCodec().decode(withBytes, null);
        assertThat(out.get("b").asString()).isEqualTo("AQI=");
        assertThat(out.get("a").get(0).asString()).isEqualTo("Aw==");
        assertThatThrownBy(() -> new CborCodec().decode(new byte[]{(byte) 0x1C}, null)).isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> new CborCodec().decode(new byte[0], null)).isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> new CborCodec().decode(utf8("{\"a\":1}"), null)).isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    @DisplayName("DSC-09.07 MessagePack: nil·큰 정수·바이너리·배열·숫자 키·타임스탬프·확장, 값 두 개는 오류")
    void msgpackRules() throws Exception {
        MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
        p.packMapHeader(7);
        p.packString("n").packNil();
        p.packString("big").packBigInteger(new java.math.BigInteger("18446744073709551615"));
        p.packString("bin").packBinaryHeader(2).writePayload(new byte[]{1, 2});
        p.packString("arr").packArrayHeader(2).packFloat(1.5f).packLong(-3);
        p.packInt(5).packString("numkey");
        p.packString("ts").packTimestamp(Instant.parse("2026-10-05T00:00:00Z"));
        p.packString("ext").packExtensionTypeHeader((byte) 3, 1).writePayload(new byte[]{9});
        JsonNode out = new MsgpackCodec().decode(p.toByteArray(), null);
        assertThat(out.get("n").isNull()).isTrue();
        assertThat(out.get("big").bigIntegerValue().toString()).isEqualTo("18446744073709551615");
        assertThat(out.get("bin").asString()).isEqualTo("AQI=");
        assertThat(out.get("arr").get(1).asLong()).isEqualTo(-3);
        assertThat(out.get("5").asString()).isEqualTo("numkey");
        assertThat(out.get("ts").asString()).isEqualTo("2026-10-05T00:00:00Z");
        assertThat(out.get("ext").get("extType").asInt()).isEqualTo(3);
        MessageBufferPacker two = MessagePack.newDefaultBufferPacker();
        two.packInt(1).packInt(2);
        assertThatThrownBy(() -> new MsgpackCodec().decode(two.toByteArray(), null)).hasMessageContaining("남는");
        assertThatThrownBy(() -> new MsgpackCodec().decode(new byte[0], null)).isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> new MsgpackCodec().decode(new byte[]{(byte) 0xC1}, null))
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    @DisplayName("DSC-09.07 CSV: 머리글·따옴표·숫자·불·빈칸·BOM·CRLF, 머리글 없음, 세미콜론·탭 구분자, 열 수 불일치 오류")
    void csvRules() throws Exception {
        JsonNode out = new CsvCodec(',', true).decode(utf8("﻿device,temp,ok,note,empty\r\n"
                + "em-1,22.5,true,\"a, \"\"b\"\"\nc\",\r\nem-2,-3,FALSE,x,\n"), null);
        JsonNode rows = out.get("rows");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("device").asString()).isEqualTo("em-1");
        assertThat(rows.get(0).get("temp").asDouble()).isEqualTo(22.5);
        assertThat(rows.get(0).get("ok").asBoolean()).isTrue();
        assertThat(rows.get(0).get("note").asString()).isEqualTo("a, \"b\"\nc");
        assertThat(rows.get(0).get("empty").isNull()).isTrue();
        assertThat(rows.get(1).get("temp").asLong()).isEqualTo(-3);
        assertThat(rows.get(1).get("ok").asBoolean()).isFalse();
        JsonNode noHeader = new CsvCodec(';', false).decode(utf8("1;2e3;x"), null);
        assertThat(noHeader.get("rows").get(0).get(1).asDouble()).isEqualTo(2000.0);
        assertThat(new CsvCodec('\t', true).decode(utf8("a\tb\n1\t2"), null).get("rows").get(0).get("b").asInt()).isEqualTo(2);
        assertThatThrownBy(() -> new CsvCodec(',', true).decode(utf8("a,b\n1"), null)).hasMessageContaining("열 수");
        assertThatThrownBy(() -> new CsvCodec(',', true).decode(utf8("a\n\"x"), null)).hasMessageContaining("따옴표");
        assertThatThrownBy(() -> new CsvCodec(',', true).decode(utf8("\n\n"), null)).hasMessageContaining("행이 없");
        assertThatThrownBy(() -> new CsvCodec(',', false).decode(utf8("1\n".repeat(10_002)), null)).hasMessageContaining("10000");
    }

    @Test
    @DisplayName("DSC-09.07 Avro: 파일 형식(OCF) 1건·여러 건, union·enum·map·array·bytes·fixed·timestamp, 남는 바이트·레지스트리 형식 아님·스키마 없음")
    void avroRules() throws Exception {
        Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"u","type":["null","double"]},{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                  {"name":"m","type":{"type":"map","values":"long"}},{"name":"a","type":{"type":"array","items":"float"}},
                  {"name":"b","type":"bytes"},{"name":"f","type":{"type":"fixed","name":"F","size":2}},
                  {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
                  {"name":"tu","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"n","type":"null"},
                  {"name":"l","type":"long"}]}""");
        GenericRecord r = new GenericData.Record(s);
        r.put("u", null);
        r.put("e", new GenericData.EnumSymbol(s.getField("e").schema(), "B"));
        r.put("m", Map.of("k", 4L));
        r.put("a", List.of(1.5f));
        r.put("b", ByteBuffer.wrap(new byte[]{1, 2}));
        r.put("f", new GenericData.Fixed(s.getField("f").schema(), new byte[]{3, 4}));
        r.put("ts", 1_780_000_000_000L);
        r.put("tu", 1_780_000_000_000_001L);
        r.put("n", null);
        r.put("l", 9L);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> w = new DataFileWriter<>(new GenericDatumWriter<>(s))) {
            w.create(s, file);
            w.append(r);
        }
        JsonNode one = new AvroCodec(null, null).decode(file.toByteArray(), null);
        assertThat(one.get("u").isNull()).isTrue();
        assertThat(one.get("e").asString()).isEqualTo("B");
        assertThat(one.get("m").get("k").asLong()).isEqualTo(4);
        assertThat(one.get("a").get(0).asDouble()).isEqualTo(1.5);
        assertThat(one.get("b").asString()).isEqualTo("AQI=");
        assertThat(one.get("f").asString()).isEqualTo("AwQ=");
        assertThat(one.get("ts").asString()).isEqualTo(Instant.ofEpochMilli(1_780_000_000_000L).toString());
        assertThat(one.get("tu").asString()).isEqualTo(Instant.ofEpochSecond(1_780_000_000L, 1000).toString());
        assertThat(one.get("l").asLong()).isEqualTo(9);
        ByteArrayOutputStream two = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> w = new DataFileWriter<>(new GenericDatumWriter<>(s))) {
            w.create(s, two);
            w.append(r);
            w.append(r);
        }
        assertThat(new AvroCodec(null, null).decode(two.toByteArray(), null).get("records")).hasSize(2);

        byte[] datum = PayloadTestData.avroDatum();
        byte[] extra = java.util.Arrays.copyOf(datum, datum.length + 1);
        assertThatThrownBy(() -> new AvroCodec(PayloadTestData.avroSchema(), null).decode(extra, null)).hasMessageContaining("남는");
        assertThatThrownBy(() -> new AvroCodec(null, id -> PayloadTestData.avroSchema()).decode(datum, null))
                .hasMessageContaining("레지스트리 형식");
        assertThatThrownBy(() -> new AvroCodec(null, id -> null).decode(PayloadTestData.avroWire(3), null))
                .hasMessageContaining("ID 3");
        assertThatThrownBy(() -> new AvroCodec(null, null).decode(datum, null)).hasMessageContaining("스키마");
        assertThatThrownBy(() -> new AvroCodec(PayloadTestData.avroSchema(), null).decode(new byte[]{2}, null))
                .isInstanceOf(PayloadDecodeException.class);
    }

    @Test
    @DisplayName("DSC-09.07 압축: gzip·deflate가 아닌 입력, 한도 초과(압축 폭탄), 잘린 deflate는 DECODE_ERROR, 모르는 압축 이름은 설정 오류")
    void compressionRules() {
        byte[] big = PayloadTestData.gzip(new byte[200_000]);
        assertThatThrownBy(() -> Compression.GZIP.decompress(big, 1000)).hasMessageContaining("1000");
        assertThatThrownBy(() -> Compression.DEFLATE.decompress(PayloadTestData.deflate(new byte[200_000], false), 1000))
                .hasMessageContaining("1000");
        assertThatThrownBy(() -> Compression.GZIP.decompress(utf8("{}"), 1000)).isInstanceOf(PayloadDecodeException.class);
        assertThatThrownBy(() -> Compression.DEFLATE.decompress(new byte[]{(byte) 0xFF, 0x12, 0x34}, 1000))
                .isInstanceOf(PayloadDecodeException.class);
        byte[] whole = PayloadTestData.deflate(utf8("hello world hello world"), true);
        assertThatThrownBy(() -> Compression.DEFLATE.decompress(java.util.Arrays.copyOf(whole, 3), 1000))
                .isInstanceOf(PayloadDecodeException.class);
        assertThat(Compression.of(null)).isEqualTo(Compression.NONE);
        assertThat(Compression.of("zlib")).isEqualTo(Compression.DEFLATE);
        assertThat(Compression.of(" gzip ")).isEqualTo(Compression.GZIP);
        assertThat(Compression.of("none")).isEqualTo(Compression.NONE);
        assertThatThrownBy(() -> Compression.of("brotli")).isInstanceOf(IllegalArgumentException.class);
    }
}
