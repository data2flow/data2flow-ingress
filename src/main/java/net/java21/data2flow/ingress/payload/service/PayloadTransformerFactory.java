package net.java21.data2flow.ingress.payload.service;

import com.google.protobuf.Descriptors.FileDescriptor;
import net.java21.data2flow.ingress.payload.codec.AvroCodec;
import net.java21.data2flow.ingress.payload.codec.CborCodec;
import net.java21.data2flow.ingress.payload.codec.CsvCodec;
import net.java21.data2flow.ingress.payload.codec.MsgpackCodec;
import net.java21.data2flow.ingress.payload.codec.PayloadCodec;
import net.java21.data2flow.ingress.payload.codec.PayloadDecodeException;
import net.java21.data2flow.ingress.payload.codec.ProtobufCodec;
import net.java21.data2flow.ingress.payload.codec.SparkplugBCodec;
import net.java21.data2flow.ingress.payload.domain.PayloadSettings;
import net.java21.data2flow.ingress.payload.schema.AvroRegistryClient;
import net.java21.data2flow.ingress.payload.schema.PayloadSchema;
import net.java21.data2flow.ingress.payload.schema.PayloadSchemaClient;
import net.java21.data2flow.ingress.payload.schema.ProtoSchemaParser;
import net.java21.data2flow.ingress.payload.schema.SchemaInvalidException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * 소스 설정에서 {@link PayloadTransformer}를 만든다(DSC-09.07·09.08). 설정 오류는 {@link IllegalArgumentException}(메시지 = 필드 이름)
 * 이라서 호출자가 SOURCE_CONFIG_INVALID로 보고한다.
 */
public class PayloadTransformerFactory {

    private final PayloadSchemaClient schemas;
    private final AvroRegistryClient registry;
    private final int maxDecompressedBytes;
    private final JsonMapper json;

    public PayloadTransformerFactory(PayloadSchemaClient schemas, AvroRegistryClient registry, int maxDecompressedBytes,
                                     JsonMapper json) {
        this.schemas = schemas;
        this.registry = registry;
        this.maxDecompressedBytes = maxDecompressedBytes;
        this.json = json;
    }

    /**
     * @param organizationId 소스 조직(업로드 스키마가 같은 조직 것인지 확인)
     * @param connectorKey   커넥터 키
     * @param config         소스 설정
     */
    public PayloadTransformer create(long organizationId, String connectorKey, JsonNode config) {
        PayloadSettings settings = PayloadSettings.from(config, connectorKey);
        return new PayloadTransformer(settings, () -> codec(organizationId, settings), maxDecompressedBytes, json);
    }

    PayloadCodec codec(long organizationId, PayloadSettings s) throws PayloadDecodeException {
        return switch (s.format()) {
            case CBOR -> new CborCodec();
            case MSGPACK -> new MsgpackCodec();
            case CSV -> new CsvCodec(s.csvDelimiter(), s.csvHeader());
            case SPARKPLUG_B -> new SparkplugBCodec();
            case PROTOBUF -> {
                PayloadSchema schema = uploaded(organizationId, s.schemaRef(), "PROTOBUF");
                List<FileDescriptor> files = ProtoSchemaParser.parse(schema.fileName(), schema.content());
                yield new ProtobufCodec(ProtoSchemaParser.find(files, s.messageType()));
            }
            case AVRO -> {
                Schema fixed = null;
                if (s.schemaRef() != null) {
                    PayloadSchema schema = uploaded(organizationId, s.schemaRef(), "AVRO");
                    try {
                        fixed = AvroCodec.parseSchema(new String(schema.content(), StandardCharsets.UTF_8));
                    } catch (SchemaParseException e) {
                        throw new SchemaInvalidException(".avsc를 해석할 수 없습니다: " + e.getMessage(), e);
                    }
                }
                String url = s.registryUrl();
                yield new AvroCodec(fixed, url == null ? null : id -> registry.schema(url, id));
            }
            case JSON, TEXT, BINARY -> throw new IllegalStateException("변환하지 않는 형식: " + s.format());
        };
    }

    private PayloadSchema uploaded(long organizationId, String schemaRef, String format) throws PayloadDecodeException {
        PayloadSchema schema = schemas.get(schemaRef).orElseThrow(() ->
                new PayloadDecodeException("업로드한 스키마 " + schemaRef + "이(가) 없습니다(API-DSC-59로 다시 올리세요)"));
        if (schema.organizationId() != organizationId) {
            throw new PayloadDecodeException("스키마 " + schemaRef + "은(는) 이 조직의 것이 아닙니다");
        }
        if (!schema.format().isBlank() && !format.equals(schema.format().toUpperCase(Locale.ROOT))) {
            throw new PayloadDecodeException("스키마 " + schemaRef + "은(는) " + schema.format() + " 스키마입니다(소스 형식 "
                    + format + ")");
        }
        return schema;
    }
}
