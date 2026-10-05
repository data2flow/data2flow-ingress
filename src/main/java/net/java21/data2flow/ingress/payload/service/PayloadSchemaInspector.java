package net.java21.data2flow.ingress.payload.service;

import com.google.protobuf.Descriptors.FileDescriptor;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.ingress.common.IngressErrorCode;
import net.java21.data2flow.ingress.payload.codec.AvroCodec;
import net.java21.data2flow.ingress.payload.dto.SchemaInspectRequest;
import net.java21.data2flow.ingress.payload.dto.SchemaInspectResponse;
import net.java21.data2flow.ingress.payload.schema.ProtoSchemaParser;
import net.java21.data2flow.ingress.payload.schema.SchemaInvalidException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * 올린 payload 스키마 검사(API-DSC-82, TC-DSC-280·281). 수집 때 쓰는 해석기({@link ProtoSchemaParser}, Avro {@code Schema.Parser})와
 * 같은 것을 써서, 저장은 됐는데 수집에서 못 읽는 스키마가 생기지 않게 한다.
 */
public class PayloadSchemaInspector {

    public static final int MAX_BYTES = 1024 * 1024;

    public SchemaInspectResponse inspect(SchemaInspectRequest request) {
        byte[] content;
        try {
            content = Base64.getDecoder().decode(request.content());
        } catch (IllegalArgumentException e) {
            throw invalid("content가 base64가 아닙니다");
        }
        if (content.length > MAX_BYTES) {
            throw invalid("파일이 " + MAX_BYTES + "바이트를 넘습니다");
        }
        String format = request.format().toUpperCase(Locale.ROOT);
        try {
            if (format.equals("PROTOBUF")) {
                List<FileDescriptor> files = ProtoSchemaParser.parse(request.fileName(), content);
                List<String> types = ProtoSchemaParser.messageTypes(files);
                String chosen = null;
                if (request.messageType() != null && !request.messageType().isBlank()) {
                    chosen = ProtoSchemaParser.find(files, request.messageType()).getFullName();
                } else {
                    try {
                        chosen = ProtoSchemaParser.find(files, null).getFullName();
                    } catch (SchemaInvalidException ambiguous) {
                        // 여럿이면 사용자가 고른다(messageType 없이 저장하면 수집 때 DECODE_ERROR)
                    }
                }
                if (types.isEmpty()) {
                    throw new SchemaInvalidException("스키마에 메시지 타입이 없습니다");
                }
                return new SchemaInspectResponse(format, types, chosen);
            }
            Schema schema = AvroCodec.parseSchema(new String(content, StandardCharsets.UTF_8));
            return new SchemaInspectResponse(format, List.of(schema.getFullName()), schema.getFullName());
        } catch (SchemaInvalidException | SchemaParseException e) {
            throw invalid(e.getMessage());
        }
    }

    private static BusinessException invalid(String message) {
        return new BusinessException(IngressErrorCode.SOURCE_SCHEMA_INVALID, message);
    }
}
