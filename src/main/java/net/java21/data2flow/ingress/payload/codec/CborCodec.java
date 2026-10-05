package net.java21.data2flow.ingress.payload.codec;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.BinaryNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.dataformat.cbor.CBORMapper;

import java.util.Base64;
import java.util.Map;

/**
 * CBOR(RFC 8949) → JSON(DSC-09.07). 바이트 문자열은 base64 문자열로, 태그는 값만 남긴다. 값이 하나보다 많거나 남는 바이트가 있으면 오류.
 * 라이브러리: Jackson dataformat-cbor(Apache-2.0).
 */
public final class CborCodec implements PayloadCodec {

    private static final CBORMapper CBOR = CBORMapper.builder()
            .enable(tools.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        try {
            JsonNode tree = CBOR.readTree(payload);
            if (tree == null || tree.isMissingNode()) {
                throw new PayloadDecodeException("CBOR 값이 없습니다");
            }
            return binaryToBase64(tree);
        } catch (JacksonException e) {
            throw new PayloadDecodeException("CBOR로 읽을 수 없습니다: " + e.getOriginalMessage(), e);
        }
    }

    static JsonNode binaryToBase64(JsonNode node) {
        if (node instanceof BinaryNode b) {
            return JsonNodeFactory.instance.stringNode(Base64.getEncoder().encodeToString(b.binaryValue()));
        }
        if (node.isObject()) {
            var out = JsonNodeFactory.instance.objectNode();
            for (Map.Entry<String, JsonNode> e : node.properties()) {
                out.set(e.getKey(), binaryToBase64(e.getValue()));
            }
            return out;
        }
        if (node.isArray()) {
            var out = JsonNodeFactory.instance.arrayNode();
            node.forEach(n -> out.add(binaryToBase64(n)));
            return out;
        }
        return node;
    }
}
