package net.java21.data2flow.ingress.payload.domain;

import net.java21.data2flow.contracts.connector.PayloadFormat;
import tools.jackson.databind.JsonNode;

import java.util.Locale;

/**
 * 소스의 payload 형식·토픽 템플릿 설정(DSC-09.07·09.08). core 실행 설정(API-DSC-50) {@code config}의 아래 필드를 읽는다
 * (DSC domain-model {@code data_sources.payload}·{@code topic_template}, connectors.md §3).
 * <pre>{@code
 * "payload": {"format": "PROTOBUF", "compression": "GZIP", "schemaRef": "42", "messageType": "acme.Reading",
 *             "registryUrl": "http://apicurio:8080/apis/ccompat/v7", "csv": {"delimiter": ",", "header": true}},
 * "topicTemplate": "site/{site}/room/{room}/{deviceId}/{metric}"
 * }</pre>
 * 형식은 대소문자 구분 없이 {@link PayloadFormat} 이름({@code MESSAGEPACK}·{@code SPARKPLUG-B}도 받는다). 없으면 JSON, 단
 * {@code sparkplug-b} 커넥터는 SPARKPLUG_B. 잘못된 값은 {@link IllegalArgumentException}(메시지는 필드 이름) → SOURCE_CONFIG_INVALID.
 *
 * @param format       형식
 * @param compression  압축
 * @param schemaRef    API-DSC-59로 올린 스키마 참조(PROTOBUF 필수, AVRO는 이것 또는 registryUrl)
 * @param messageType  Protobuf 메시지 타입(없으면 스키마의 유일한 최상위 메시지)
 * @param registryUrl  Avro 스키마 레지스트리(Confluent 호환 REST) 주소
 * @param csvDelimiter CSV 구분자(기본 쉼표)
 * @param csvHeader    CSV 첫 줄이 머리글인가(기본 true)
 * @param topicTemplate 토픽 템플릿. 없으면 null
 */
public record PayloadSettings(PayloadFormat format, Compression compression, String schemaRef, String messageType,
                              String registryUrl, char csvDelimiter, boolean csvHeader, TopicTemplate topicTemplate) {

    public static final PayloadSettings PASSTHROUGH = new PayloadSettings(PayloadFormat.JSON, Compression.NONE, null, null,
            null, ',', true, null);

    /** 변환·추출할 것이 없다(받은 그대로 기록) */
    public boolean passthrough() {
        return compression == Compression.NONE && topicTemplate == null
                && (format == PayloadFormat.JSON || format == PayloadFormat.TEXT || format == PayloadFormat.BINARY);
    }

    /** 구조화된 JSON으로 바꾸는 형식인가(JSON·TEXT·BINARY는 압축만 풀고 그대로 넘긴다 — BINARY는 디코더 스크립트가 읽는다) */
    public boolean converts() {
        return format != PayloadFormat.JSON && format != PayloadFormat.TEXT && format != PayloadFormat.BINARY;
    }

    /**
     * @param config       소스 설정 JSON
     * @param connectorKey 커넥터 키(기본 형식을 정할 때)
     */
    public static PayloadSettings from(JsonNode config, String connectorKey) {
        JsonNode p = config == null ? null : config.path("payload");
        String formatText = p != null && p.path("format").isString() ? p.get("format").asString() : null;
        PayloadFormat format = formatText == null || formatText.isBlank()
                ? ("sparkplug-b".equals(connectorKey) ? PayloadFormat.SPARKPLUG_B : PayloadFormat.JSON)
                : format(formatText);
        Compression compression;
        try {
            compression = Compression.of(p != null && p.path("compression").isString() ? p.get("compression").asString() : null);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("payload.compression");
        }
        String schemaRef = text(p, "schemaRef");
        String messageType = text(p, "messageType");
        String registryUrl = text(p, "registryUrl");
        if (format == PayloadFormat.PROTOBUF && schemaRef == null) {
            throw new IllegalArgumentException("payload.schemaRef");
        }
        if (format == PayloadFormat.AVRO && schemaRef == null && registryUrl == null) {
            throw new IllegalArgumentException("payload.registryUrl");
        }
        if (registryUrl != null && !registryUrl.matches("(?i)https?://\\S+")) {
            throw new IllegalArgumentException("payload.registryUrl");
        }
        char delimiter = ',';
        boolean header = true;
        JsonNode csv = p == null ? null : p.path("csv");
        if (csv != null && csv.isObject()) {
            String d = csv.path("delimiter").asString(",");
            if ("\\t".equals(d) || "tab".equalsIgnoreCase(d)) {
                d = "\t";
            }
            if (d.length() != 1 || d.charAt(0) == '"' || d.charAt(0) == '\n' || d.charAt(0) == '\r') {
                throw new IllegalArgumentException("payload.csv.delimiter");
            }
            delimiter = d.charAt(0);
            header = csv.path("header").asBoolean(true);
        }
        String template = config == null ? null : text(config, "topicTemplate");
        TopicTemplate topicTemplate;
        try {
            topicTemplate = template == null ? null : TopicTemplate.compile(template);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("topicTemplate");
        }
        return new PayloadSettings(format, compression, schemaRef, messageType, registryUrl == null ? null
                : registryUrl.replaceAll("/+$", ""), delimiter, header, topicTemplate);
    }

    static PayloadFormat format(String text) {
        String v = text.strip().toUpperCase(Locale.ROOT).replace('-', '_');
        return switch (v) {
            case "MESSAGEPACK" -> PayloadFormat.MSGPACK;
            case "SPARKPLUG", "SPARKPLUGB" -> PayloadFormat.SPARKPLUG_B;
            case "PROTO" -> PayloadFormat.PROTOBUF;
            default -> {
                try {
                    yield PayloadFormat.valueOf(v);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("payload.format");
                }
            }
        };
    }

    private static String text(JsonNode node, String field) {
        if (node == null || !node.path(field).isValueNode() || node.get(field).isNull()) {
            return null;
        }
        String v = node.get(field).asString().strip();
        return v.isEmpty() ? null : v;
    }
}
