package net.java21.data2flow.ingress.payload.codec;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Avro → JSON(DSC-09.07). 세 가지 모양을 읽는다(앞에서부터 확인).
 * <ol>
 *   <li><b>Object Container File</b>({@code Obj\x01}로 시작): 파일 안의 스키마로 읽는다. 레코드가 여럿이면 {@code {"records": [...]}}.</li>
 *   <li><b>스키마 레지스트리 형식</b>(레지스트리 주소가 있을 때): {@code 0x00} + 스키마 ID(4바이트 big-endian) + 본문. Confluent 호환
 *       REST({@code GET {registryUrl}/schemas/ids/{id}}, Apicurio {@code /apis/ccompat/v7})로 스키마를 가져온다.</li>
 *   <li><b>본문만</b>(업로드한 .avsc가 있을 때): 그 스키마로 읽는다.</li>
 * </ol>
 * 변환: 레코드는 객체, union은 값만, enum은 이름, bytes·fixed는 base64, {@code timestamp-millis·micros}는 ISO-8601 문자열.
 * 남는 바이트가 있으면 오류. 라이브러리: Apache Avro(Apache-2.0). Confluent 직렬화 라이브러리(Confluent Community License)는 쓰지 않는다.
 */
public final class AvroCodec implements PayloadCodec {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;
    private static final byte[] OCF_MAGIC = {'O', 'b', 'j', 1};
    private static final int MAX_RECORDS = 10_000;

    private final Schema schema;
    private final IntFunction<Schema> registry;

    /**
     * @param schema   업로드한 스키마(.avsc). 없으면 null
     * @param registry 스키마 ID → 스키마(레지스트리 조회). 없으면 null. 조회가 일시적으로 실패하면 {@link SchemaUnavailableException}
     */
    public AvroCodec(Schema schema, IntFunction<Schema> registry) {
        this.schema = schema;
        this.registry = registry;
    }

    /** .avsc 문서를 스키마로(SOURCE_SCHEMA_INVALID 판정에도 쓴다) */
    public static Schema parseSchema(String avsc) {
        return new Schema.Parser().parse(avsc);
    }

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        try {
            if (startsWith(payload, OCF_MAGIC)) {
                return container(payload);
            }
            if (registry != null) {
                if (payload.length < 5 || payload[0] != 0) {
                    throw new PayloadDecodeException("스키마 레지스트리 형식(0x00 + 스키마 ID 4바이트)이 아닙니다");
                }
                int id = ByteBuffer.wrap(payload, 1, 4).getInt();
                Schema writer = registry.apply(id);
                if (writer == null) {
                    throw new PayloadDecodeException("스키마 레지스트리에 ID " + id + " 스키마가 없습니다");
                }
                return datum(writer, payload, 5);
            }
            if (schema == null) {
                throw new PayloadDecodeException("Avro 스키마(업로드 또는 레지스트리 주소)가 없습니다");
            }
            return datum(schema, payload, 0);
        } catch (IOException | AvroRuntimeException | ClassCastException | IndexOutOfBoundsException
                 | NegativeArraySizeException e) {
            throw new PayloadDecodeException("Avro로 읽을 수 없습니다: " + e.getMessage(), e);
        }
    }

    private static JsonNode datum(Schema writer, byte[] payload, int offset) throws IOException, PayloadDecodeException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, offset, payload.length - offset, null);
        Object value = new GenericDatumReader<>(writer, writer, GenericData.get()).read(null, decoder);
        if (!decoder.isEnd()) {
            throw new PayloadDecodeException("Avro 값 뒤에 남는 바이트가 있습니다(스키마가 맞지 않을 수 있습니다)");
        }
        return toJson(value, writer);
    }

    private static JsonNode container(byte[] payload) throws IOException, PayloadDecodeException {
        try (DataFileStream<Object> stream = new DataFileStream<>(new ByteArrayInputStream(payload),
                new GenericDatumReader<>())) {
            Schema s = stream.getSchema();
            ArrayNode records = F.arrayNode();
            for (Object o : stream) {
                records.add(toJson(o, s));
                if (records.size() > MAX_RECORDS) {
                    throw new PayloadDecodeException("Avro 파일 레코드가 " + MAX_RECORDS + "개를 넘습니다");
                }
            }
            if (records.size() == 1) {
                return records.get(0);
            }
            ObjectNode out = F.objectNode();
            out.set("records", records);
            return out;
        }
    }

    static JsonNode toJson(Object v, Schema s) {
        if (v == null) {
            return F.nullNode();
        }
        return switch (s.getType()) {
            case UNION -> toJson(v, s.getTypes().get(GenericData.get().resolveUnion(s, v)));
            case RECORD -> {
                GenericRecord r = (GenericRecord) v;
                ObjectNode out = F.objectNode();
                for (Schema.Field f : s.getFields()) {
                    out.set(f.name(), toJson(r.get(f.pos()), f.schema()));
                }
                yield out;
            }
            case ARRAY -> {
                ArrayNode out = F.arrayNode();
                for (Object item : (Collection<?>) v) {
                    out.add(toJson(item, s.getElementType()));
                }
                yield out;
            }
            case MAP -> {
                ObjectNode out = F.objectNode();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    out.set(String.valueOf(e.getKey()), toJson(e.getValue(), s.getValueType()));
                }
                yield out;
            }
            case ENUM, STRING -> F.stringNode(v.toString());
            case BYTES -> {
                ByteBuffer b = ((ByteBuffer) v).duplicate();
                byte[] bytes = new byte[b.remaining()];
                b.get(bytes);
                yield F.stringNode(Base64.getEncoder().encodeToString(bytes));
            }
            case FIXED -> F.stringNode(Base64.getEncoder().encodeToString(((GenericFixed) v).bytes()));
            case INT -> F.numberNode((Integer) v);
            case LONG -> {
                LogicalType lt = s.getLogicalType();
                long l = (Long) v;
                if (lt instanceof LogicalTypes.TimestampMillis) {
                    yield F.stringNode(Instant.ofEpochMilli(l).toString());
                }
                if (lt instanceof LogicalTypes.TimestampMicros) {
                    yield F.stringNode(Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000L),
                            Math.floorMod(l, 1_000_000L) * 1000).toString());
                }
                yield F.numberNode(l);
            }
            case FLOAT -> F.numberNode((Float) v);
            case DOUBLE -> F.numberNode((Double) v);
            case BOOLEAN -> F.booleanNode((Boolean) v);
            case NULL -> F.nullNode();
        };
    }

    private static boolean startsWith(byte[] payload, byte[] magic) {
        if (payload.length < magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (payload[i] != magic[i]) {
                return false;
            }
        }
        return true;
    }
}
