package net.java21.data2flow.ingress.payload.schema;

import net.java21.data2flow.ingress.payload.AvroRegistryStub;
import net.java21.data2flow.ingress.payload.PayloadTestData;
import net.java21.data2flow.ingress.payload.codec.SchemaUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07 UC-DSC-16 2: Avro 스키마 레지스트리(Apicurio ccompat·Confluent 호환 REST) 조회와 캐시 */
class AvroRegistryClientTest {

    final AvroRegistryStub stub = new AvroRegistryStub();
    final AvroRegistryClient client = new AvroRegistryClient(Duration.ofSeconds(2), PayloadTestData.JSON);

    @AfterEach
    void close() {
        stub.close();
    }

    @Test
    @DisplayName("DSC-09.07 ID로 스키마를 읽고 (주소, ID)로 캐시한다. 없으면 null, Avro가 아닌 스키마도 null")
    void readsAndCaches() {
        stub.avro(7, PayloadTestData.AVSC).raw(8, "{\"schemaType\":\"PROTOBUF\",\"schema\":\"syntax\"}");
        assertThat(client.schema(stub.url(), 7).getFullName()).isEqualTo("acme.Reading");
        assertThat(client.schema(stub.url(), 7).getFullName()).isEqualTo("acme.Reading");
        assertThat(stub.requests()).isEqualTo(1);
        assertThat(client.schema(stub.url(), 404)).isNull();
        assertThat(client.schema(stub.url(), 8)).isNull();
    }

    @Test
    @DisplayName("DSC-09.03 5xx·접속 실패는 일시 오류(기록 실패 → 재전송), 해석할 수 없는 스키마는 SchemaInvalid")
    void failures() {
        stub.status(5, 503).raw(6, "{\"schema\":\"{not avro\"}");
        assertThatThrownBy(() -> client.schema(stub.url(), 5)).isInstanceOf(SchemaUnavailableException.class)
                .hasMessageContaining("503");
        assertThatThrownBy(() -> client.schema(stub.url(), 6)).isInstanceOf(SchemaInvalidException.class);
        assertThatThrownBy(() -> client.schema("http://127.0.0.1:1", 1)).isInstanceOf(SchemaUnavailableException.class);
    }
}
