package net.java21.data2flow.ingress.payload.codec;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * CSV(RFC 4180, UTF-8) → {@code {"rows": [...]}}(DSC-09.07). 머리글이 있으면 각 행이 {@code {열: 값}} 객체, 없으면 값 배열이다.
 * 값은 정수·실수·true/false면 그 타입으로, 빈 칸은 null, 나머지는 문자열. 따옴표 안의 구분자·줄바꿈·{@code ""}를 처리한다.
 * 디코더(generic-json)는 {@code $.rows[*]} 같은 경로로 읽는다.
 */
public final class CsvCodec implements PayloadCodec {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;
    private static final Pattern NUMBER = Pattern.compile("-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?");
    private static final int MAX_ROWS = 10_000;

    private final char delimiter;
    private final boolean header;

    public CsvCodec(char delimiter, boolean header) {
        this.delimiter = delimiter;
        this.header = header;
    }

    @Override
    public JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException {
        String text = new String(payload, StandardCharsets.UTF_8);
        if (text.startsWith("﻿")) {
            text = text.substring(1);
        }
        List<List<String>> rows = parse(text);
        if (rows.isEmpty()) {
            throw new PayloadDecodeException("CSV에 행이 없습니다");
        }
        ObjectNode out = F.objectNode();
        ArrayNode array = out.putArray("rows");
        List<String> columns = header ? rows.remove(0) : null;
        for (int r = 0; r < rows.size(); r++) {
            List<String> row = rows.get(r);
            if (columns == null) {
                ArrayNode values = array.addArray();
                row.forEach(v -> values.add(typed(v)));
            } else {
                if (row.size() != columns.size()) {
                    throw new PayloadDecodeException("CSV " + (r + 2) + "번째 줄의 열 수(" + row.size() + ")가 머리글("
                            + columns.size() + ")과 다릅니다");
                }
                ObjectNode obj = array.addObject();
                for (int c = 0; c < columns.size(); c++) {
                    obj.set(columns.get(c).strip(), typed(row.get(c)));
                }
            }
        }
        return out;
    }

    private static JsonNode typed(String v) {
        String s = v.strip();
        if (s.isEmpty()) {
            return F.nullNode();
        }
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return F.booleanNode(Boolean.parseBoolean(s));
        }
        if (NUMBER.matcher(s).matches()) {
            BigDecimal d = new BigDecimal(s);
            if (d.scale() <= 0 && !s.contains("e") && !s.contains("E") && d.abs().compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0) {
                return F.numberNode(d.longValueExact());
            }
            return F.numberNode(d.doubleValue());
        }
        return F.stringNode(v);
    }

    private List<List<String>> parse(String text) throws PayloadDecodeException {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        boolean any = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cell.append(c);
                }
            } else if (c == '"' && cell.isEmpty()) {
                quoted = true;
                any = true;
            } else if (c == delimiter) {
                row.add(cell.toString());
                cell.setLength(0);
                any = true;
            } else if (c == '\n' || c == '\r') {
                if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') {
                    i++;
                }
                if (any || !cell.isEmpty()) {
                    row.add(cell.toString());
                    rows.add(row);
                    if (rows.size() > MAX_ROWS) {
                        throw new PayloadDecodeException("CSV 행이 " + MAX_ROWS + "개를 넘습니다");
                    }
                }
                row = new ArrayList<>();
                cell.setLength(0);
                any = false;
            } else {
                cell.append(c);
                any = true;
            }
        }
        if (quoted) {
            throw new PayloadDecodeException("CSV 따옴표가 닫히지 않았습니다");
        }
        if (any || !cell.isEmpty()) {
            row.add(cell.toString());
            rows.add(row);
        }
        return rows;
    }
}
