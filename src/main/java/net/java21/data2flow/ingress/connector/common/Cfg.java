package net.java21.data2flow.ingress.connector.common;

import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 커넥터 설정 JSON(DSC domain-model §2.2, connectors.md §3)을 읽는 도우미. 값이 틀리면 필드 이름만 담은
 * {@link InvalidSettingsException}(SOURCE_CONFIG_INVALID·SOURCE_SECRET_REQUIRED)을 던진다. 값(비밀값일 수 있음)은 메시지에 넣지 않는다.
 */
public final class Cfg {

    private final SourceConfig source;
    private final JsonNode node;

    private Cfg(SourceConfig source, JsonNode node) {
        this.source = source;
        this.node = node;
    }

    public static Cfg of(SourceConfig source) {
        return new Cfg(source, source.config());
    }

    /** 하위 객체(없으면 빈 객체처럼 읽힌다) */
    public Cfg child(String field) {
        return new Cfg(source, node.path(field));
    }

    public JsonNode node() {
        return node;
    }

    public SourceConfig source() {
        return source;
    }

    public String text(String field, String fallback) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() || !v.isValueNode() || v.asString("").isBlank() ? fallback : v.asString().trim();
    }

    public String required(String field) {
        String v = text(field, null);
        if (v == null) {
            throw InvalidSettingsException.config(field);
        }
        return v;
    }

    public int integer(String field, int fallback, int min, int max) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        if (!v.isNumber() && !(v.isString() && v.asString().matches("-?\\d+"))) {
            throw InvalidSettingsException.config(field);
        }
        int value = v.asInt();
        if (value < min || value > max) {
            throw InvalidSettingsException.config(field);
        }
        return value;
    }

    public long longValue(String field, long fallback) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        if (!v.isNumber() && !(v.isString() && v.asString().matches("-?\\d+"))) {
            throw InvalidSettingsException.config(field);
        }
        return v.asLong();
    }

    public double decimal(String field, double fallback) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        if (!v.isNumber()) {
            throw InvalidSettingsException.config(field);
        }
        return v.asDouble();
    }

    public boolean bool(String field, boolean fallback) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? fallback : v.asBoolean(fallback);
    }

    /** 문자열 배열(또는 쉼표로 나눈 문자열) */
    public List<String> strings(String field) {
        JsonNode v = node.get(field);
        List<String> out = new ArrayList<>();
        if (v == null || v.isNull()) {
            return out;
        }
        if (v.isString()) {
            for (String s : v.asString().split(",")) {
                if (!s.isBlank()) {
                    out.add(s.trim());
                }
            }
            return out;
        }
        for (JsonNode e : v) {
            if (!e.isValueNode() || e.asString("").isBlank()) {
                throw InvalidSettingsException.config(field);
            }
            out.add(e.asString().trim());
        }
        return out;
    }

    /** 초 단위 설정을 Duration으로(최소값 아래면 오류) */
    public Duration seconds(String field, long fallbackSec, long minSec, long maxSec) {
        long v = longValue(field, fallbackSec);
        if (v < minSec || v > maxSec) {
            throw InvalidSettingsException.config(field);
        }
        return Duration.ofSeconds(v);
    }

    public URI uri(String field) {
        String raw = required(field);
        try {
            URI uri = new URI(raw);
            if (uri.getScheme() == null || uri.getHost() == null) {
                throw InvalidSettingsException.config(field);
            }
            return uri;
        } catch (URISyntaxException e) {
            throw InvalidSettingsException.config(field);
        }
    }

    /** 비밀값(종류 이름은 대소문자·밑줄 무시). 없으면 null */
    public Secret secret(String kind) {
        return secret(source, kind);
    }

    /** 비밀값의 원문. 없으면 null */
    public String reveal(String kind) {
        Secret s = secret(kind);
        return s == null ? null : s.reveal();
    }

    public Secret requiredSecret(String kind) {
        Secret s = secret(kind);
        if (s == null) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, kind);
        }
        return s;
    }

    public static Secret secret(SourceConfig source, String kind) {
        for (Map.Entry<String, Secret> e : source.secrets().entrySet()) {
            if (e.getKey().equalsIgnoreCase(kind) || e.getKey().replace("_", "").equalsIgnoreCase(kind.replace("_", ""))) {
                return e.getValue();
            }
        }
        return null;
    }
}
