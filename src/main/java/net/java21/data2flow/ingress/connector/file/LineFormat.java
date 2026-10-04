package net.java21.data2flow.ingress.connector.file;

import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 파일 한 줄 → 원본 하나(DSC-09 "파일·일괄"). {@code jsonl}은 줄을 그대로, {@code csv}는 머리글 이름을 키로 한 JSON 객체(숫자로 읽히면 숫자)로
 * 바꾼다(RFC 4180 따옴표·쉼표 처리). Parquet은 아직 지원하지 않는다.
 */
public enum LineFormat {
    JSONL, CSV;

    private static final JsonMapper JSON = JsonMapper.builder().build();

    public static LineFormat of(String value) {
        try {
            return LineFormat.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw InvalidSettingsException.config("format");
        }
    }

    /** 머리글이 있는 형식인가(첫 줄은 기록하지 않는다) */
    public boolean hasHeader() {
        return this == CSV;
    }

    /** 줄 하나를 payload로. 빈 줄은 null */
    public byte[] payload(String line, List<String> header) {
        if (line.isBlank()) {
            return null;
        }
        if (this == JSONL) {
            return line.getBytes(StandardCharsets.UTF_8);
        }
        List<String> cells = split(line);
        ObjectNode o = JSON.createObjectNode();
        for (int i = 0; i < header.size(); i++) {
            String v = i < cells.size() ? cells.get(i) : "";
            if (v.matches("-?\\d{1,18}")) {
                o.put(header.get(i), Long.parseLong(v));
            } else if (v.matches("-?\\d+\\.\\d+([eE][-+]?\\d+)?")) {
                o.put(header.get(i), Double.parseDouble(v));
            } else {
                o.put(header.get(i), v);
            }
        }
        return JSON.writeValueAsBytes(o);
    }

    /** CSV 한 줄을 칸으로(따옴표 안 쉼표·두 겹 따옴표) */
    public static List<String> split(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (quoted) {
                if (ch == '"' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else if (ch == '"') {
                    quoted = false;
                } else {
                    cur.append(ch);
                }
            } else if (ch == '"') {
                quoted = true;
            } else if (ch == ',') {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString().trim());
        return out;
    }
}
