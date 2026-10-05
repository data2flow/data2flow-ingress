package net.java21.data2flow.ingress.payload.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * Protobuf(업로드한 스키마의 메시지 타입, API-DSC-59) → JSON(DSC-09.07, AT-DSC-16.1).
 *
 * <p>규칙(proto3 JSON 매핑을 따르되 센서 값에 맞게 바꾼 곳이 있다):
 * <ul>
 *   <li>필드 이름은 JSON 이름(lowerCamelCase 또는 {@code json_name}).</li>
 *   <li><b>값이 0·false·""인 단일 필드도 싣는다</b>(proto3 JSON은 생략하지만 온도 0도가 사라지면 안 된다). 존재 여부가 있는 필드
 *       (메시지·oneof·optional)는 있을 때만 싣는다. repeated는 비어도 [] 로 싣는다.</li>
 *   <li>64비트 정수는 문자열이 아니라 숫자, 부호 없는 정수는 부호 없이 읽는다. bytes는 base64, enum은 이름(모르는 번호는 숫자).</li>
 *   <li>map은 객체, {@code google.protobuf.Timestamp}는 ISO-8601 UTC 문자열, 래퍼 타입은 값 그대로.</li>
 *   <li>스키마에 없는 필드 번호는 버리지 않고 {@code _unknownFields}에 번호 목록으로 남긴다(스키마가 오래됐다는 신호).</li>
 * </ul>
 */
public final class ProtobufCodec implements PayloadCodec {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private final Descriptor type;

    public ProtobufCodec(Descriptor type) {
        this.type = type;
    }

    public Descriptor type() {
        return type;
    }

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        DynamicMessage message;
        try {
            message = DynamicMessage.parseFrom(type, payload);
        } catch (InvalidProtocolBufferException e) {
            throw new PayloadDecodeException("Protobuf " + type.getFullName() + "로 읽을 수 없습니다: " + e.getMessage(), e);
        }
        return toJson(message);
    }

    /** 메시지 하나를 JSON 객체로(Sparkplug B 등 다른 코덱도 쓴다) */
    public static JsonNode toJson(Message message) {
        Descriptor d = message.getDescriptorForType();
        switch (d.getFullName()) {
            case "google.protobuf.Timestamp" -> {
                long seconds = (long) message.getField(d.findFieldByName("seconds"));
                int nanos = (int) message.getField(d.findFieldByName("nanos"));
                return F.stringNode(Instant.ofEpochSecond(seconds, nanos).toString());
            }
            case "google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int64Value",
                 "google.protobuf.UInt64Value", "google.protobuf.Int32Value", "google.protobuf.UInt32Value",
                 "google.protobuf.BoolValue", "google.protobuf.StringValue", "google.protobuf.BytesValue" -> {
                FieldDescriptor value = d.findFieldByName("value");
                return scalar(value, message.getField(value));
            }
            default -> {
                // 일반 메시지
            }
        }
        ObjectNode out = F.objectNode();
        for (FieldDescriptor f : d.getFields()) {
            if (f.isRepeated()) {
                if (f.isMapField()) {
                    ObjectNode map = out.putObject(f.getJsonName());
                    FieldDescriptor key = f.getMessageType().findFieldByNumber(1);
                    FieldDescriptor value = f.getMessageType().findFieldByNumber(2);
                    for (Object entry : (List<?>) message.getField(f)) {
                        Message e = (Message) entry;
                        map.set(String.valueOf(scalarValue(key, e.getField(key))), value(value, e.getField(value)));
                    }
                } else {
                    ArrayNode array = out.putArray(f.getJsonName());
                    for (Object item : (List<?>) message.getField(f)) {
                        array.add(value(f, item));
                    }
                }
            } else if (!f.hasPresence() || message.hasField(f)) {
                out.set(f.getJsonName(), value(f, message.getField(f)));
            }
        }
        if (!message.getUnknownFields().asMap().isEmpty()) {
            ArrayNode unknown = out.putArray("_unknownFields");
            message.getUnknownFields().asMap().keySet().forEach(unknown::add);
        }
        return out;
    }

    private static JsonNode value(FieldDescriptor f, Object v) {
        if (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return toJson((Message) v);
        }
        return scalar(f, v);
    }

    private static JsonNode scalar(FieldDescriptor f, Object v) {
        Object s = scalarValue(f, v);
        return switch (s) {
            case Integer i -> F.numberNode(i);
            case Long l -> F.numberNode(l);
            case BigInteger b -> F.numberNode(b);
            case Float x -> F.numberNode(x);
            case Double x -> F.numberNode(x);
            case Boolean b -> F.booleanNode(b);
            default -> F.stringNode(String.valueOf(s));
        };
    }

    private static Object scalarValue(FieldDescriptor f, Object v) {
        return switch (f.getType()) {
            case UINT32, FIXED32 -> Integer.toUnsignedLong((Integer) v);
            case UINT64, FIXED64 -> (Long) v >= 0 ? v : new BigInteger(Long.toUnsignedString((Long) v));
            case BYTES -> Base64.getEncoder().encodeToString(((ByteString) v).toByteArray());
            case ENUM -> {
                EnumValueDescriptor e = (EnumValueDescriptor) v;
                yield e.getIndex() < 0 || e.getType().findValueByNumber(e.getNumber()) == null ? e.getNumber() : e.getName();
            }
            default -> v;
        };
    }
}
