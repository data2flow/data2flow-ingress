package net.java21.data2flow.ingress.payload.codec;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

/**
 * MessagePack → JSON(DSC-09.07). 맵 키는 문자열로, 바이너리는 base64로, 확장 타입은 {@code {"extType": n, "data": base64}}로 바꾼다.
 * 타임스탬프 확장(-1)은 ISO-8601 문자열. 값이 하나보다 많으면 오류. 라이브러리: msgpack-core(Apache-2.0).
 */
public final class MsgpackCodec implements PayloadCodec {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(payload)) {
            if (!unpacker.hasNext()) {
                throw new PayloadDecodeException("MessagePack 값이 없습니다");
            }
            Value value = unpacker.unpackValue();
            if (unpacker.hasNext()) {
                throw new PayloadDecodeException("MessagePack 값 뒤에 남는 바이트가 있습니다");
            }
            return toJson(value);
        } catch (IOException | RuntimeException e) {
            throw new PayloadDecodeException("MessagePack으로 읽을 수 없습니다: " + e.getMessage(), e);
        }
    }

    private static JsonNode toJson(Value v) {
        return switch (v.getValueType()) {
            case NIL -> F.nullNode();
            case BOOLEAN -> F.booleanNode(v.asBooleanValue().getBoolean());
            case INTEGER -> v.asIntegerValue().isInLongRange() ? F.numberNode(v.asIntegerValue().toLong())
                    : F.numberNode(v.asIntegerValue().toBigInteger());
            case FLOAT -> F.numberNode(v.asFloatValue().toDouble());
            case STRING -> F.stringNode(v.asStringValue().asString());
            case BINARY -> F.stringNode(Base64.getEncoder().encodeToString(v.asBinaryValue().asByteArray()));
            case ARRAY -> {
                ArrayValue a = v.asArrayValue();
                ArrayNode out = F.arrayNode();
                a.forEach(item -> out.add(toJson(item)));
                yield out;
            }
            case MAP -> {
                MapValue m = v.asMapValue();
                ObjectNode out = F.objectNode();
                for (Map.Entry<Value, Value> e : m.entrySet()) {
                    Value k = e.getKey();
                    out.set(k.isStringValue() ? k.asStringValue().asString() : k.toJson(), toJson(e.getValue()));
                }
                yield out;
            }
            case EXTENSION -> {
                ExtensionValue x = v.asExtensionValue();
                if (x.isTimestampValue()) {
                    yield F.stringNode(x.asTimestampValue().toInstant().toString());
                }
                ObjectNode out = F.objectNode();
                out.put("extType", x.getType());
                out.put("data", Base64.getEncoder().encodeToString(x.getData()));
                yield out;
            }
        };
    }
}
