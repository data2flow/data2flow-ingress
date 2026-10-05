package net.java21.data2flow.ingress.payload.schema;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TimestampProto;
import net.java21.data2flow.ingress.payload.PayloadTestData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.07 TC-DSC-280 API-DSC-59·82: 올린 .proto·FileDescriptorSet 해석 */
class ProtoSchemaParserTest {

    static List<FileDescriptor> parse(String text) {
        return ProtoSchemaParser.parse("x.proto", text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("DSC-09.07 TC-DSC-280 proto3: 중첩 메시지·enum·map·oneof·optional·repeated·json_name·표준 타입 import·주석을 읽는다")
    void parsesProto3() {
        List<FileDescriptor> files = parse("""
                /* 여러 줄
                   주석 */
                syntax = "proto3";
                package acme.v1;
                import public "google/protobuf/timestamp.proto";
                option (my.opt) = { a: 1 };
                service Svc { rpc Get(Reading) returns (Reading) { option deprecated = true; } }
                enum Kind { option allow_alias = true; KIND_UNSPECIFIED = 0; TEMP = 1 [deprecated = true]; NEG = -1; reserved 5; }
                message Reading {
                  reserved 9, 10 to 12; reserved "old";
                  option deprecated = true;
                  message Point { double v = 1; }
                  string device_id = 1 [json_name = "dev"];
                  repeated Point points = 2 [packed = true, (custom).x = "y"];
                  map<string, double> tags = 3;
                  map<int32, Point> byIndex = 4;
                  oneof value { option (o) = 1; int64 count = 5; string text = 6; }
                  optional uint32 battery = 7;
                  google.protobuf.Timestamp at = 8;
                  .acme.v1.Kind kind = 13;
                  bytes raw = 0x0E;
                  extend Foo { int32 bar = 100; }
                }
                extend Foo { int32 baz = 101; }
                """);
        Descriptor r = ProtoSchemaParser.find(files, "acme.v1.Reading");
        assertThat(ProtoSchemaParser.messageTypes(files)).containsExactly("acme.v1.Reading", "acme.v1.Reading.Point");
        assertThat(r.findFieldByName("device_id").getJsonName()).isEqualTo("dev");
        assertThat(r.findFieldByName("points").getMessageType().getName()).isEqualTo("Point");
        assertThat(r.findFieldByName("tags").isMapField()).isTrue();
        assertThat(r.findFieldByName("byIndex").getMessageType().getName()).isEqualTo("ByIndexEntry");
        assertThat(r.findFieldByName("count").getContainingOneof().getName()).isEqualTo("value");
        assertThat(r.findFieldByName("battery").hasPresence()).isTrue();
        assertThat(r.findFieldByName("at").getMessageType()).isEqualTo(TimestampProto.getDescriptor().getMessageTypes().get(0));
        assertThat(r.findFieldByName("kind").getEnumType().findValueByNumber(-1).getName()).isEqualTo("NEG");
        assertThat(r.findFieldByName("raw").getNumber()).isEqualTo(14);
        assertThat(ProtoSchemaParser.find(files, "Point").getFullName()).isEqualTo("acme.v1.Reading.Point");
        assertThat(ProtoSchemaParser.find(files, ".acme.v1.Reading")).isEqualTo(r);
    }

    @Test
    @DisplayName("DSC-09.07 proto2: required·default·8진수 번호, syntax 없으면 proto2")
    void parsesProto2() {
        List<FileDescriptor> files = parse("""
                message A { required int32 id = 1; optional string name = 2 [default = "x"]; optional double t = 3 [default = -1.5];
                  repeated sint64 s = 010; optional B b = 4; }
                message B { optional E e = 1 [default = ON]; enum E { OFF = 0; ON = 1; } }
                """);
        Descriptor a = ProtoSchemaParser.find(files, "A");
        assertThat(a.findFieldByName("id").isRequired()).isTrue();
        assertThat(a.findFieldByName("name").getDefaultValue()).isEqualTo("x");
        assertThat(a.findFieldByName("t").getDefaultValue()).isEqualTo(-1.5);
        assertThat(a.findFieldByName("s").getNumber()).isEqualTo(8);
        assertThat(a.findFieldByName("b").getType()).isEqualTo(FieldDescriptor.Type.MESSAGE);
        assertThatThrownBy(() -> ProtoSchemaParser.find(files, null)).isInstanceOf(SchemaInvalidException.class)
                .hasMessageContaining("messageType");
    }

    @Test
    @DisplayName("DSC-09.07 FileDescriptorSet(이진, protoc --include_imports) 업로드도 읽는다")
    void parsesDescriptorSet() {
        FileDescriptor reading = PayloadTestData.readingDescriptor().getFile();
        byte[] set = FileDescriptorSet.newBuilder().addFile(TimestampProto.getDescriptor().toProto())
                .addFile(reading.toProto()).build().toByteArray();
        List<FileDescriptor> files = ProtoSchemaParser.parse("reading.desc", set);
        assertThat(ProtoSchemaParser.messageTypes(files)).containsExactly("acme.Reading");
        assertThat(ProtoSchemaParser.find(files, null).getFullName()).isEqualTo("acme.Reading");
        // 확장자 없이도 UTF-8이 아니면 이진으로 본다
        assertThat(ProtoSchemaParser.messageTypes(ProtoSchemaParser.parse("schema", set))).containsExactly("acme.Reading");
    }

    @Test
    @DisplayName("DSC-09.07 FileDescriptorSet 오류: 깨진 이진·빈 집합·빠진 import")
    void descriptorSetErrors() {
        assertThatThrownBy(() -> ProtoSchemaParser.parse("a.desc", new byte[]{(byte) 0xFF, 0x01}))
                .isInstanceOf(SchemaInvalidException.class);
        assertThatThrownBy(() -> ProtoSchemaParser.parse("a.pb", FileDescriptorSet.newBuilder().build().toByteArray()))
                .isInstanceOf(SchemaInvalidException.class);
        byte[] missing = FileDescriptorSet.newBuilder().addFile(PayloadTestData.readingDescriptor().getFile().toProto()
                .toBuilder().addDependency("other.proto")).build().toByteArray();
        assertThatThrownBy(() -> ProtoSchemaParser.parse("a.binpb", missing)).hasMessageContaining("other.proto");
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "빈 파일||비어",
            "syntax가 뒤에|message A { optional int32 a = 1; } syntax = \"proto3\";|syntax",
            "모르는 syntax|syntax = \"proto4\";|proto4",
            "editions|edition = \"2023\";|editions",
            "사용자 import|syntax = \"proto3\"; import \"other.proto\";|other.proto",
            "group|message A { optional group G = 1 { } }|group",
            "proto3 required|syntax = \"proto3\"; message A { required int32 a = 1; }|required",
            "proto2 라벨 없음|message A { int32 a = 1; }|optional",
            "세미콜론 없음|syntax = \"proto3\"; message A { int32 a = 1 }|';'",
            "모르는 타입|syntax = \"proto3\"; message A { Nope a = 1; }|Nope",
            "번호 중복|syntax = \"proto3\"; message A { int32 a = 1; int32 b = 1; }|검증",
            "map 키 double|syntax = \"proto3\"; message A { map<double, int32> m = 1; }|map",
            "닫히지 않은 주석|/* 끝 없음|주석",
            "닫히지 않은 문자열|syntax = \"proto3;|문자열",
            "정수 아님|syntax = \"proto3\"; message A { int32 a = x; }|정수",
            "이름 아님|syntax = \"proto3\"; message 1A { }|이름",
            "빈 enum|enum E { }|enum",
            "모르는 선언|foo bar;|foo",
            "일찍 끝남|syntax = \"proto3\"; message A {|일찍",
            "메시지 없음|syntax = \"proto3\"; enum E { A = 0; }|메시지"})
    @DisplayName("DSC-09.07 TC-DSC-281 SOURCE_SCHEMA_INVALID: 해석할 수 없는 .proto는 원인과 함께 거부")
    void rejects(String name, String text, String reason) {
        assertThatThrownBy(() -> {
            List<FileDescriptor> files = ProtoSchemaParser.parse("x.proto",
                    text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8));
            ProtoSchemaParser.find(files, null);
        }).isInstanceOf(SchemaInvalidException.class).hasMessageContaining(reason);
    }

    @Test
    @DisplayName("DSC-09.07 없는 메시지 타입은 있는 타입 목록과 함께 거부, 오류에 줄 번호")
    void unknownTypeAndLineNumber() {
        List<FileDescriptor> files = parse("syntax = \"proto3\"; message A { int32 a = 1; }");
        assertThatThrownBy(() -> ProtoSchemaParser.find(files, "B")).hasMessageContaining("[A]");
        assertThatThrownBy(() -> parse("syntax = \"proto3\";\n\nmessage A {\n  int32 a = ;\n}"))
                .hasMessageStartingWith("4번째 줄");
    }
}
