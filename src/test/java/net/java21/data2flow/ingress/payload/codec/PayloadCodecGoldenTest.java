package net.java21.data2flow.ingress.payload.codec;

import net.java21.data2flow.ingress.payload.domain.Compression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static net.java21.data2flow.ingress.payload.PayloadTestData.avroDatum;
import static net.java21.data2flow.ingress.payload.PayloadTestData.avroSchema;
import static net.java21.data2flow.ingress.payload.PayloadTestData.avroWire;
import static net.java21.data2flow.ingress.payload.PayloadTestData.cbor;
import static net.java21.data2flow.ingress.payload.PayloadTestData.deflate;
import static net.java21.data2flow.ingress.payload.PayloadTestData.golden;
import static net.java21.data2flow.ingress.payload.PayloadTestData.gzip;
import static net.java21.data2flow.ingress.payload.PayloadTestData.json;
import static net.java21.data2flow.ingress.payload.PayloadTestData.msgpack;
import static net.java21.data2flow.ingress.payload.PayloadTestData.normalize;
import static net.java21.data2flow.ingress.payload.PayloadTestData.protobuf;
import static net.java21.data2flow.ingress.payload.PayloadTestData.readingDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSC-09.07 TC-DSC-284 AT-DSC-16.1·16.2: JSON·CBOR·MessagePack·Protobuf(업로드 스키마)·Avro(레지스트리 스텁)·gzip·deflate 골든 입력이
 * 모두 같은 구조화 값이 된다. Sparkplug B는 {@link SparkplugBCodecTest}.
 */
class PayloadCodecGoldenTest {

    static final int MAX = 1024 * 1024;

    @Test
    @DisplayName("DSC-09.07 TC-DSC-284 CBOR·MessagePack·Protobuf·Avro(업로드·레지스트리) → 같은 구조화 값")
    void formatsDecodeToSameValue() throws Exception {
        JsonNode expected = golden();
        assertThat(normalize(new CborCodec().decode(cbor(), null))).isEqualTo(expected);
        assertThat(normalize(new MsgpackCodec().decode(msgpack(), null))).isEqualTo(expected);
        assertThat(normalize(new ProtobufCodec(readingDescriptor()).decode(protobuf(), null))).isEqualTo(expected);
        assertThat(normalize(new AvroCodec(avroSchema(), null).decode(avroDatum(), null))).isEqualTo(expected);
        assertThat(normalize(new AvroCodec(null, id -> id == 7 ? avroSchema() : null).decode(avroWire(7), null)))
                .as("Apicurio·Confluent 호환 레지스트리 형식(0x00 + ID)").isEqualTo(expected);
    }

    @Test
    @DisplayName("DSC-09.07 AT-DSC-16.2 gzip·deflate(zlib·원시) 압축 JSON과 압축 CBOR도 풀면 같은 값")
    void compressedDecodes() throws Exception {
        assertThat(normalize(Compression.GZIP.decompress(gzip(json()), MAX))).isEqualTo(golden());
        assertThat(normalize(Compression.DEFLATE.decompress(deflate(json(), false), MAX))).isEqualTo(golden());
        assertThat(normalize(Compression.DEFLATE.decompress(deflate(json(), true), MAX))).isEqualTo(golden());
        assertThat(normalize(new CborCodec().decode(Compression.GZIP.decompress(gzip(cbor()), MAX), null))).isEqualTo(golden());
        assertThat(Compression.NONE.decompress(json(), MAX)).isEqualTo(json());
    }
}
