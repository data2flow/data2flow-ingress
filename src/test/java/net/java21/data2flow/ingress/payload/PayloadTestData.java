package net.java21.data2flow.ingress.payload;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import net.java21.data2flow.ingress.payload.schema.ProtoSchemaParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.cbor.CBORMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * DSC-09.07 골든 입력(TC-DSC-284): 같은 값 {@code {"deviceId":"em-1","temperature":22.5,"humidity":41,"ok":true}}을 형식마다 만든다.
 */
public final class PayloadTestData {

    public static final JsonMapper JSON = JsonMapper.builder().build();
    public static final String GOLDEN_JSON = "{\"deviceId\":\"em-1\",\"temperature\":22.5,\"humidity\":41,\"ok\":true}";
    public static final String PROTO = """
            // 계량기 측정값
            syntax = "proto3";
            package acme;
            import "google/protobuf/timestamp.proto";
            option java_package = "x.y";

            message Reading {
              string device_id = 1;
              double temperature = 2;
              int32 humidity = 3;
              bool ok = 4;
            }
            """;
    public static final String AVSC = """
            {"type":"record","name":"Reading","namespace":"acme","fields":[
              {"name":"deviceId","type":"string"},{"name":"temperature","type":"double"},
              {"name":"humidity","type":"int"},{"name":"ok","type":"boolean"}]}
            """;

    private PayloadTestData() {
    }

    public static JsonNode golden() {
        return JSON.readTree(GOLDEN_JSON);
    }

    /** 숫자 타입 차이(IntNode·LongNode)를 없애고 비교하려고 JSON 문자열로 한 번 돌린다 */
    public static JsonNode normalize(JsonNode node) {
        return JSON.readTree(JSON.writeValueAsString(node));
    }

    public static JsonNode normalize(byte[] json) {
        return JSON.readTree(json);
    }

    public static byte[] json() {
        return GOLDEN_JSON.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] cbor() {
        return new CBORMapper().writeValueAsBytes(golden());
    }

    public static byte[] msgpack() {
        try (MessageBufferPacker p = MessagePack.newDefaultBufferPacker()) {
            p.packMapHeader(4);
            p.packString("deviceId").packString("em-1");
            p.packString("temperature").packDouble(22.5);
            p.packString("humidity").packInt(41);
            p.packString("ok").packBoolean(true);
            return p.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Descriptor readingDescriptor() {
        return ProtoSchemaParser.find(ProtoSchemaParser.parse("reading.proto", PROTO.getBytes(StandardCharsets.UTF_8)),
                "Reading");
    }

    public static byte[] protobuf() {
        Descriptor d = readingDescriptor();
        return DynamicMessage.newBuilder(d)
                .setField(d.findFieldByName("device_id"), "em-1")
                .setField(d.findFieldByName("temperature"), 22.5)
                .setField(d.findFieldByName("humidity"), 41)
                .setField(d.findFieldByName("ok"), true).build().toByteArray();
    }

    public static Schema avroSchema() {
        return new Schema.Parser().parse(AVSC);
    }

    public static GenericRecord avroRecord() {
        GenericRecord r = new GenericData.Record(avroSchema());
        r.put("deviceId", "em-1");
        r.put("temperature", 22.5);
        r.put("humidity", 41);
        r.put("ok", true);
        return r;
    }

    /** 본문만(업로드 스키마로 읽음) */
    public static byte[] avroDatum() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(avroSchema()).write(avroRecord(), enc);
            enc.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 레지스트리 형식: 0x00 + 스키마 ID + 본문 */
    public static byte[] avroWire(int schemaId) {
        byte[] body = avroDatum();
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(schemaId).put(body).array();
    }

    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
                gz.write(data);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** @param raw true면 머리 없는 원시 deflate */
    public static byte[] deflate(byte[] data, boolean raw) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DeflaterOutputStream d = new DeflaterOutputStream(out, new Deflater(Deflater.DEFAULT_COMPRESSION, raw))) {
                d.write(data);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Sparkplug B 메트릭 하나 */
    public record SpMetric(String name, Long alias, Integer datatype, Object value) {
    }

    /** Sparkplug B Payload(timestamp, metrics, seq)를 공개 규격 필드 번호로 만든다 */
    public static byte[] sparkplug(long timestamp, long seq, SpMetric... metrics) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CodedOutputStream c = CodedOutputStream.newInstance(out);
            c.writeUInt64(1, timestamp);
            for (SpMetric m : metrics) {
                ByteArrayOutputStream mb = new ByteArrayOutputStream();
                CodedOutputStream mc = CodedOutputStream.newInstance(mb);
                if (m.name() != null) {
                    mc.writeString(1, m.name());
                }
                if (m.alias() != null) {
                    mc.writeUInt64(2, m.alias());
                }
                if (m.datatype() != null) {
                    mc.writeUInt32(4, m.datatype());
                }
                switch (m.value()) {
                    case null -> mc.writeBool(7, true);
                    case Integer i -> mc.writeUInt32(10, i);
                    case Long l -> mc.writeUInt64(11, l);
                    case Float f -> mc.writeFloat(12, f);
                    case Double d -> mc.writeDouble(13, d);
                    case Boolean b -> mc.writeBool(14, b);
                    case String s -> mc.writeString(15, s);
                    case byte[] b -> mc.writeByteArray(16, b);
                    default -> throw new IllegalArgumentException(String.valueOf(m.value()));
                }
                mc.flush();
                c.writeByteArray(2, mb.toByteArray());
            }
            c.writeUInt64(3, seq);
            c.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
