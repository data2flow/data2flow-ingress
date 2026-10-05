package net.java21.data2flow.ingress.payload.codec;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sparkplug B(Eclipse Sparkplug 3.0) payload → JSON(DSC-09.07, AT-DSC-16.3).
 *
 * <p>토픽 {@code spBv1.0/{group}/{type}/{edgeNode}[/{device}]}. payload는 Protobuf {@code Payload{timestamp=1, metrics=2, seq=3, uuid=4,
 * body=5}}이고 {@code Metric{name=1, alias=2, timestamp=3, datatype=4, is_historical=5, is_transient=6, is_null=7, …, int_value=10,
 * long_value=11, float_value=12, double_value=13, boolean_value=14, string_value=15, bytes_value=16, dataset_value=17,
 * template_value=18}}이다. 공개 규격의 필드 번호로 직접 읽는다(Eclipse Tahu 코드를 넣지 않는다).
 *
 * <p><b>별칭:</b> NBIRTH·DBIRTH가 알려 준 {@code alias → (이름, 자료형)}을 엣지 노드별로 기억하고, DATA의 별칭 메트릭에 이름과 자료형을
 * 채운다. NBIRTH가 오면 그 노드의 별칭을 비운다(규격: 노드가 다시 태어나면 별칭이 다시 정해진다). 재시작 직후처럼 BIRTH를 못 본 별칭은
 * 이름 없이 {@code alias}만 싣는다 — 우리는 브로커에 발행하지 않으므로(CLAUDE.md §5) Rebirth를 요청하지 않는다. 별칭 상태는 메모리라서
 * 인스턴스마다 따로이고 재시작하면 사라진다.
 *
 * <p>결과: {@code {messageType, groupId, edgeNodeId, deviceId?, timestamp, seq, uuid?, metrics:[{name, alias?, timestamp?, datatype,
 * value, isNull?, isHistorical?, isTransient?}]}}. DataSet·Template 값은 {@code value: null, unsupported: true}로 남긴다.
 */
public final class SparkplugBCodec implements PayloadCodec {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;
    private static final String[] TYPES = {"Unknown", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
            "Float", "Double", "Boolean", "String", "DateTime", "Text", "UUID", "DataSet", "Bytes", "File", "Template",
            "PropertySet", "PropertySetList", "Int8Array", "Int16Array", "Int32Array", "Int64Array", "UInt8Array",
            "UInt16Array", "UInt32Array", "UInt64Array", "FloatArray", "DoubleArray", "BooleanArray", "StringArray",
            "DateTimeArray"};

    /** 별칭 하나 */
    record AliasInfo(String name, int datatype) {
    }

    /** {@code group/edgeNode} → (alias → 정보) */
    private final Map<String, Map<Long, AliasInfo>> aliases = new ConcurrentHashMap<>();

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        String[] t = topic == null ? new String[0] : topic.split("/");
        if (t.length < 4 || !"spBv1.0".equals(t[0])) {
            throw new PayloadDecodeException("Sparkplug B 토픽(spBv1.0/{group}/{type}/{edgeNode}[/{device}])이 아닙니다");
        }
        String messageType = t[2];
        String node = t[1] + "/" + t[3];
        ObjectNode out = F.objectNode();
        out.put("messageType", messageType);
        out.put("groupId", t[1]);
        out.put("edgeNodeId", t[3]);
        if (t.length > 4) {
            out.put("deviceId", t[4]);
        }
        boolean birth = messageType.endsWith("BIRTH");
        if ("NBIRTH".equals(messageType)) {
            aliases.put(node, new ConcurrentHashMap<>());
        }
        Map<Long, AliasInfo> known = aliases.computeIfAbsent(node, k -> new ConcurrentHashMap<>());
        ArrayNode metrics = F.arrayNode();
        try {
            CodedInputStream in = CodedInputStream.newInstance(payload);
            while (true) {
                int tag = in.readTag();
                if (tag == 0) {
                    break;
                }
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> out.put("timestamp", in.readUInt64());
                    case 2 -> {
                        int limit = in.pushLimit(in.readRawVarint32());
                        metrics.add(metric(in, birth, known));
                        in.popLimit(limit);
                    }
                    case 3 -> out.put("seq", in.readUInt64());
                    case 4 -> out.put("uuid", in.readString());
                    case 5 -> out.put("body", Base64.getEncoder().encodeToString(in.readByteArray()));
                    default -> in.skipField(tag);
                }
            }
        } catch (IOException | RuntimeException e) {
            throw new PayloadDecodeException("Sparkplug B payload를 읽을 수 없습니다: " + e.getMessage(), e);
        }
        out.set("metrics", metrics);
        return out;
    }

    private static ObjectNode metric(CodedInputStream in, boolean birth, Map<Long, AliasInfo> known) throws IOException {
        String name = null;
        Long alias = null;
        Long timestamp = null;
        Integer datatype = null;
        boolean isNull = false;
        boolean historical = false;
        boolean transientValue = false;
        JsonNode raw = null;
        Object number = null;
        boolean unsupported = false;
        while (true) {
            int tag = in.readTag();
            if (tag == 0) {
                break;
            }
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> name = in.readString();
                case 2 -> alias = in.readUInt64();
                case 3 -> timestamp = in.readUInt64();
                case 4 -> datatype = in.readUInt32();
                case 5 -> historical = in.readBool();
                case 6 -> transientValue = in.readBool();
                case 7 -> isNull = in.readBool();
                case 10 -> number = in.readUInt32();
                case 11 -> number = in.readUInt64();
                case 12 -> raw = F.numberNode(in.readFloat());
                case 13 -> raw = F.numberNode(in.readDouble());
                case 14 -> raw = F.booleanNode(in.readBool());
                case 15 -> raw = F.stringNode(in.readString());
                case 16 -> raw = F.stringNode(Base64.getEncoder().encodeToString(in.readByteArray()));
                case 17, 18 -> {
                    in.skipField(tag);
                    unsupported = true;
                }
                default -> in.skipField(tag);
            }
        }
        if (alias != null) {
            if (birth && name != null) {
                known.put(alias, new AliasInfo(name, datatype == null ? 0 : datatype));
            } else if (name == null || datatype == null) {
                AliasInfo info = known.get(alias);
                if (info != null) {
                    name = name == null ? info.name() : name;
                    datatype = datatype == null ? info.datatype() : datatype;
                }
            }
        }
        ObjectNode m = F.objectNode();
        if (name != null) {
            m.put("name", name);
        } else {
            m.putNull("name");
        }
        if (alias != null) {
            m.put("alias", alias);
        }
        if (timestamp != null) {
            m.put("timestamp", timestamp);
        }
        int dt = datatype == null ? 0 : datatype;
        m.put("datatype", dt >= 0 && dt < TYPES.length ? TYPES[dt] : Integer.toString(dt));
        if (isNull || unsupported) {
            m.putNull("value");
        } else if (number != null) {
            m.set("value", integer(number, dt));
        } else if (raw != null) {
            m.set("value", raw);
        } else {
            m.putNull("value");
        }
        if (isNull) {
            m.put("isNull", true);
        }
        if (unsupported) {
            m.put("unsupported", true);
        }
        if (historical) {
            m.put("isHistorical", true);
        }
        if (transientValue) {
            m.put("isTransient", true);
        }
        return m;
    }

    /** int_value(uint32)·long_value(uint64)를 자료형에 맞게(부호 있는 형식은 2의 보수로 저장된다) */
    private static JsonNode integer(Object number, int datatype) {
        if (number instanceof Integer i) {
            return switch (datatype) {
                case 1 -> F.numberNode((byte) (int) i);
                case 2 -> F.numberNode((short) (int) i);
                case 3 -> F.numberNode(i);
                default -> F.numberNode(Integer.toUnsignedLong(i));
            };
        }
        long l = (Long) number;
        return switch (datatype) {
            case 8 -> l >= 0 ? F.numberNode(l) : F.numberNode(new BigInteger(Long.toUnsignedString(l)));
            default -> F.numberNode(l);
        };
    }

    /** 별칭을 기억하는 엣지 노드 수(시험·지표용) */
    int knownNodes() {
        return aliases.size();
    }
}
