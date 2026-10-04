package net.java21.data2flow.ingress.connector.common;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** 커넥터 설정 JSON Schema(2020-12) 읽기. 파일은 {@code classpath:/connectors/{key}.schema.json}(DSC-09.01, BR-DSC-22) */
public final class ConnectorSchemas {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private ConnectorSchemas() {
    }

    public static JsonNode load(String key) {
        String resource = "/connectors/" + key + ".schema.json";
        try (InputStream in = ConnectorSchemas.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("커넥터 스키마가 없습니다: " + resource);
            }
            return JSON.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
