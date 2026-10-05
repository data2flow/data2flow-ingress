package net.java21.data2flow.ingress.payload.schema;

import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 올린 Protobuf 스키마를 메시지 설명자로 바꾼다(DSC-09.07, API-DSC-59·82). 두 가지를 받는다.
 * <ul>
 *   <li><b>.proto 문서</b>(proto2·proto3): 메시지·중첩 메시지·enum·oneof·map·repeated·proto3 optional·proto2 default·json_name.
 *       import는 Google 표준 타입(timestamp·duration·wrappers·struct·empty·any)만 된다. service·extend·option은 읽고 버린다.
 *       group·editions는 거부한다. protoc가 필요 없는 순수 Java 해석기다(protobuf-java에는 .proto 해석기가 없다).</li>
 *   <li><b>FileDescriptorSet</b>(이진, {@code protoc --descriptor_set_out}): 여러 파일·사용자 import가 필요하면 이것을 올린다.</li>
 * </ul>
 * 이름 해석과 형식 검사는 protobuf-java({@link FileDescriptor#buildFrom})가 한다.
 */
public final class ProtoSchemaParser {

    private static final Map<String, FileDescriptor> WELL_KNOWN = Map.of(
            "google/protobuf/timestamp.proto", TimestampProto.getDescriptor(),
            "google/protobuf/duration.proto", DurationProto.getDescriptor(),
            "google/protobuf/wrappers.proto", WrappersProto.getDescriptor(),
            "google/protobuf/struct.proto", StructProto.getDescriptor(),
            "google/protobuf/empty.proto", EmptyProto.getDescriptor(),
            "google/protobuf/any.proto", AnyProto.getDescriptor());

    private static final Map<String, FieldDescriptorProto.Type> SCALARS = Map.ofEntries(
            Map.entry("double", FieldDescriptorProto.Type.TYPE_DOUBLE),
            Map.entry("float", FieldDescriptorProto.Type.TYPE_FLOAT),
            Map.entry("int64", FieldDescriptorProto.Type.TYPE_INT64),
            Map.entry("uint64", FieldDescriptorProto.Type.TYPE_UINT64),
            Map.entry("int32", FieldDescriptorProto.Type.TYPE_INT32),
            Map.entry("fixed64", FieldDescriptorProto.Type.TYPE_FIXED64),
            Map.entry("fixed32", FieldDescriptorProto.Type.TYPE_FIXED32),
            Map.entry("bool", FieldDescriptorProto.Type.TYPE_BOOL),
            Map.entry("string", FieldDescriptorProto.Type.TYPE_STRING),
            Map.entry("bytes", FieldDescriptorProto.Type.TYPE_BYTES),
            Map.entry("uint32", FieldDescriptorProto.Type.TYPE_UINT32),
            Map.entry("sfixed32", FieldDescriptorProto.Type.TYPE_SFIXED32),
            Map.entry("sfixed64", FieldDescriptorProto.Type.TYPE_SFIXED64),
            Map.entry("sint32", FieldDescriptorProto.Type.TYPE_SINT32),
            Map.entry("sint64", FieldDescriptorProto.Type.TYPE_SINT64));

    private ProtoSchemaParser() {
    }

    /**
     * @param fileName 올린 파일 이름(확장자로 이진 여부를 먼저 본다). 없으면 {@code schema.proto}
     * @param content  파일 내용
     * @return 해석한 파일들(마지막이 올린 파일). 최상위 메시지 이름은 {@link #messageTypes}
     */
    public static List<FileDescriptor> parse(String fileName, byte[] content) {
        if (content == null || content.length == 0) {
            throw new SchemaInvalidException("스키마 파일이 비어 있습니다");
        }
        String name = fileName == null || fileName.isBlank() ? "schema.proto" : fileName;
        String lower = name.toLowerCase(Locale.ROOT);
        String text = lower.endsWith(".desc") || lower.endsWith(".pb") || lower.endsWith(".binpb") ? null : utf8(content);
        if (text == null) {
            return fromDescriptorSet(content);
        }
        FileDescriptorProto proto = new Parser(text, baseName(name)).file();
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dep : proto.getDependencyList()) {
            FileDescriptor wk = WELL_KNOWN.get(dep);
            if (wk == null) {
                throw new SchemaInvalidException("import \"" + dep + "\"는 쓸 수 없습니다. Google 표준 타입만 import할 수 있고, "
                        + "여러 파일이 필요하면 protoc --descriptor_set_out --include_imports로 만든 .desc를 올리세요");
            }
            deps.add(wk);
        }
        try {
            return List.of(FileDescriptor.buildFrom(proto, deps.toArray(FileDescriptor[]::new)));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new SchemaInvalidException("스키마 검증 실패: " + e.getDescription() + " (" + e.getProblemSymbolName() + ")", e);
        }
    }

    /** 최상위·중첩 메시지 전체 이름(패키지 포함, map 항목 제외) */
    public static List<String> messageTypes(List<FileDescriptor> files) {
        List<String> names = new ArrayList<>();
        for (FileDescriptor f : files) {
            if (WELL_KNOWN.containsValue(f)) {
                continue;
            }
            for (Descriptor d : f.getMessageTypes()) {
                collect(d, names);
            }
        }
        return names;
    }

    private static void collect(Descriptor d, List<String> names) {
        if (d.getOptions().getMapEntry()) {
            return;
        }
        names.add(d.getFullName());
        for (Descriptor n : d.getNestedTypes()) {
            collect(n, names);
        }
    }

    /**
     * 메시지 타입을 찾는다. 이름은 전체 이름({@code pkg.Reading}) 또는 짧은 이름({@code Reading}). 비어 있으면 메시지가 하나일 때만 그것.
     */
    public static Descriptor find(List<FileDescriptor> files, String messageType) {
        List<String> all = messageTypes(files);
        if (all.isEmpty()) {
            throw new SchemaInvalidException("스키마에 메시지 타입이 없습니다");
        }
        String wanted = messageType == null || messageType.isBlank() ? null : messageType.strip().replaceFirst("^\\.", "");
        if (wanted == null) {
            List<String> top = new ArrayList<>();
            for (FileDescriptor f : files) {
                if (!WELL_KNOWN.containsValue(f)) {
                    f.getMessageTypes().forEach(d -> top.add(d.getFullName()));
                }
            }
            if (top.size() != 1) {
                throw new SchemaInvalidException("메시지 타입이 여럿이라 messageType을 정해야 합니다: " + top);
            }
            wanted = top.get(0);
        }
        for (FileDescriptor f : files) {
            for (String candidate : all) {
                if (candidate.equals(wanted) || candidate.endsWith("." + wanted)) {
                    Descriptor d = lookup(f, candidate);
                    if (d != null) {
                        return d;
                    }
                }
            }
        }
        throw new SchemaInvalidException("메시지 타입 " + wanted + "이(가) 스키마에 없습니다. 있는 타입: " + all);
    }

    private static Descriptor lookup(FileDescriptor f, String fullName) {
        String pkg = f.getPackage();
        String rel = pkg.isEmpty() ? fullName : fullName.startsWith(pkg + ".") ? fullName.substring(pkg.length() + 1) : null;
        if (rel == null) {
            return null;
        }
        String[] parts = rel.split("\\.");
        Descriptor d = f.findMessageTypeByName(parts[0]);
        for (int i = 1; d != null && i < parts.length; i++) {
            d = d.findNestedTypeByName(parts[i]);
        }
        return d;
    }

    private static List<FileDescriptor> fromDescriptorSet(byte[] content) {
        FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(content);
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaInvalidException("FileDescriptorSet(이진)을 읽을 수 없습니다: " + e.getMessage(), e);
        }
        if (set.getFileCount() == 0) {
            throw new SchemaInvalidException("FileDescriptorSet에 파일이 없습니다");
        }
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        set.getFileList().forEach(f -> byName.put(f.getName(), f));
        Map<String, FileDescriptor> built = new HashMap<>(WELL_KNOWN);
        List<FileDescriptor> result = new ArrayList<>();
        for (FileDescriptorProto f : set.getFileList()) {
            FileDescriptor d = build(f.getName(), byName, built, new ArrayList<>());
            if (!WELL_KNOWN.containsKey(f.getName())) {
                result.add(d);
            }
        }
        return result;
    }

    private static FileDescriptor build(String name, Map<String, FileDescriptorProto> byName, Map<String, FileDescriptor> built,
                                        List<String> path) {
        FileDescriptor done = built.get(name);
        if (done != null) {
            return done;
        }
        if (path.contains(name)) {
            throw new SchemaInvalidException("import가 순환합니다: " + path + " → " + name);
        }
        FileDescriptorProto proto = byName.get(name);
        if (proto == null) {
            throw new SchemaInvalidException("FileDescriptorSet에 " + name + "이(가) 없습니다(--include_imports로 만드세요)");
        }
        path.add(name);
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dep : proto.getDependencyList()) {
            deps.add(build(dep, byName, built, path));
        }
        path.remove(name);
        try {
            FileDescriptor d = FileDescriptor.buildFrom(proto, deps.toArray(FileDescriptor[]::new));
            built.put(name, d);
            return d;
        } catch (Descriptors.DescriptorValidationException e) {
            throw new SchemaInvalidException("스키마 검증 실패(" + name + "): " + e.getDescription(), e);
        }
    }

    private static String utf8(byte[] content) {
        try {
            String s = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(content)).toString();
            return s.indexOf('\0') >= 0 ? null : s;
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static String baseName(String name) {
        int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
        return name.substring(slash + 1);
    }

    // ---- .proto 문서 해석 ----

    private static final class Parser {
        private final Tokenizer t;
        private final String fileName;
        private boolean proto3;

        Parser(String text, String fileName) {
            this.t = new Tokenizer(text);
            this.fileName = fileName;
        }

        FileDescriptorProto file() {
            FileDescriptorProto.Builder f = FileDescriptorProto.newBuilder().setName(fileName);
            boolean first = true;
            while (!t.eof()) {
                String tok = t.next();
                switch (tok) {
                    case "syntax" -> {
                        if (!first) {
                            throw t.error("syntax는 파일 맨 앞에 와야 합니다");
                        }
                        t.expect("=");
                        String syntax = t.string();
                        t.expect(";");
                        if (!syntax.equals("proto2") && !syntax.equals("proto3")) {
                            throw t.error("지원하지 않는 syntax: " + syntax);
                        }
                        proto3 = syntax.equals("proto3");
                        f.setSyntax(syntax);
                    }
                    case "edition" -> throw t.error("editions 문법은 아직 지원하지 않습니다. proto2·proto3로 올리거나 .desc를 올리세요");
                    case "package" -> {
                        f.setPackage(t.fullIdent());
                        t.expect(";");
                    }
                    case "import" -> {
                        String next = t.peek();
                        if ("public".equals(next) || "weak".equals(next)) {
                            t.next();
                        }
                        f.addDependency(t.string());
                        t.expect(";");
                    }
                    case "option" -> skipStatement();
                    case "message" -> f.addMessageType(message());
                    case "enum" -> f.addEnumType(enumType());
                    case "service", "extend" -> {
                        t.fullIdent();
                        skipBlock();
                    }
                    case ";" -> { }
                    default -> throw t.error("알 수 없는 선언: " + tok);
                }
                first = false;
            }
            if (!f.hasSyntax()) {
                f.setSyntax("proto2");   // protoc 기본값과 같다
            }
            return f.build();
        }

        private DescriptorProto message() {
            DescriptorProto.Builder m = DescriptorProto.newBuilder().setName(t.ident());
            t.expect("{");
            while (!t.accept("}")) {
                String tok = t.next();
                switch (tok) {
                    case "message" -> m.addNestedType(message());
                    case "enum" -> m.addEnumType(enumType());
                    case "option", "reserved", "extensions" -> skipStatement();
                    case "extend" -> {
                        t.fullIdent();
                        skipBlock();
                    }
                    case "oneof" -> oneof(m);
                    case "map" -> mapField(m);
                    case ";" -> { }
                    case "optional", "required", "repeated" -> field(m, tok, t.next(), null);
                    default -> field(m, null, tok, null);
                }
            }
            return m.build();
        }

        private void oneof(DescriptorProto.Builder m) {
            int index = m.getOneofDeclCount();
            m.addOneofDecl(OneofDescriptorProto.newBuilder().setName(t.ident()));
            t.expect("{");
            while (!t.accept("}")) {
                String tok = t.next();
                if (tok.equals("option")) {
                    skipStatement();
                } else if (!tok.equals(";")) {
                    field(m, null, tok, index);
                }
            }
        }

        private void field(DescriptorProto.Builder m, String label, String type, Integer oneofIndex) {
            if (type.equals("group")) {
                throw t.error("proto2 group은 지원하지 않습니다");
            }
            if (label != null && label.equals("required") && proto3) {
                throw t.error("proto3에서는 required를 쓸 수 없습니다");
            }
            if (label == null && !proto3 && oneofIndex == null) {
                throw t.error("proto2 필드에는 optional·required·repeated가 필요합니다");
            }
            String name = t.ident();
            t.expect("=");
            int number = t.integer();
            FieldDescriptorProto.Builder fb = FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                    .setLabel("repeated".equals(label) ? FieldDescriptorProto.Label.LABEL_REPEATED
                            : "required".equals(label) ? FieldDescriptorProto.Label.LABEL_REQUIRED
                            : FieldDescriptorProto.Label.LABEL_OPTIONAL);
            FieldDescriptorProto.Type scalar = SCALARS.get(type);
            if (scalar != null) {
                fb.setType(scalar);
            } else {
                fb.setTypeName(type);   // 메시지·enum은 buildFrom이 범위 규칙으로 찾는다
            }
            fieldOptions(fb);
            t.expect(";");
            if (oneofIndex != null) {
                fb.setOneofIndex(oneofIndex);
            } else if (proto3 && "optional".equals(label)) {
                // proto3 optional: 합성 oneof(protoc와 같은 규칙)
                fb.setProto3Optional(true).setOneofIndex(m.getOneofDeclCount());
                m.addOneofDecl(OneofDescriptorProto.newBuilder().setName("_" + name));
            }
            m.addField(fb);
        }

        private void mapField(DescriptorProto.Builder m) {
            t.expect("<");
            String keyType = t.next();
            t.expect(",");
            String valueType = t.fullIdent();
            t.expect(">");
            String name = t.ident();
            t.expect("=");
            int number = t.integer();
            FieldDescriptorProto.Type key = SCALARS.get(keyType);
            if (key == null || key == FieldDescriptorProto.Type.TYPE_DOUBLE || key == FieldDescriptorProto.Type.TYPE_FLOAT
                    || key == FieldDescriptorProto.Type.TYPE_BYTES) {
                throw t.error("map 키 타입으로 쓸 수 없습니다: " + keyType);
            }
            String entryName = camel(name) + "Entry";
            FieldDescriptorProto.Builder value = FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            FieldDescriptorProto.Type scalar = SCALARS.get(valueType);
            if (scalar != null) {
                value.setType(scalar);
            } else {
                value.setTypeName(valueType);
            }
            m.addNestedType(DescriptorProto.newBuilder().setName(entryName)
                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                    .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(key)
                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                    .addField(value));
            FieldDescriptorProto.Builder fb = FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).setTypeName(entryName);
            fieldOptions(fb);
            t.expect(";");
            m.addField(fb);
        }

        /** {@code [packed = true, json_name = "x", default = 1, (custom) = …]}: json_name·default만 쓰고 나머지는 버린다 */
        private void fieldOptions(FieldDescriptorProto.Builder fb) {
            if (!t.accept("[")) {
                return;
            }
            do {
                StringBuilder name = new StringBuilder();
                while (!t.peekIs("=")) {
                    name.append(t.next());
                }
                t.expect("=");
                String value = constant();
                switch (name.toString()) {
                    case "json_name" -> fb.setJsonName(unquote(value));
                    case "default" -> fb.setDefaultValue(unquote(value));
                    default -> { }
                }
            } while (t.accept(","));
            t.expect("]");
        }

        private EnumDescriptorProto enumType() {
            EnumDescriptorProto.Builder e = EnumDescriptorProto.newBuilder().setName(t.ident());
            t.expect("{");
            while (!t.accept("}")) {
                String tok = t.next();
                if (tok.equals("option") || tok.equals("reserved")) {
                    skipStatement();
                } else if (!tok.equals(";")) {
                    t.expect("=");
                    boolean negative = t.accept("-");
                    int n = t.integer();
                    if (t.peekIs("[")) {
                        skipBrackets();
                    }
                    t.expect(";");
                    e.addValue(EnumValueDescriptorProto.newBuilder().setName(tok).setNumber(negative ? -n : n));
                }
            }
            if (e.getValueCount() == 0) {
                throw t.error("enum " + e.getName() + "에 값이 없습니다");
            }
            return e.build();
        }

        /** 상수: 문자열(따옴표 유지)·숫자(부호 포함)·식별자·{…} 묶음 */
        private String constant() {
            if (t.peekIs("{")) {
                skipBlock();
                return "";
            }
            String v = t.next();
            if ((v.equals("-") || v.equals("+")) && !t.eof()) {
                v = v + t.next();
            }
            return v;
        }

        private void skipStatement() {
            int depth = 0;
            while (true) {
                String tok = t.next();
                if (tok.equals("{") || tok.equals("[") || tok.equals("(")) {
                    depth++;
                } else if (tok.equals("}") || tok.equals("]") || tok.equals(")")) {
                    depth--;
                } else if (tok.equals(";") && depth == 0) {
                    return;
                }
            }
        }

        private void skipBlock() {
            t.expect("{");
            int depth = 1;
            while (depth > 0) {
                String tok = t.next();
                if (tok.equals("{")) {
                    depth++;
                } else if (tok.equals("}")) {
                    depth--;
                }
            }
        }

        private void skipBrackets() {
            t.expect("[");
            int depth = 1;
            while (depth > 0) {
                String tok = t.next();
                if (tok.equals("[")) {
                    depth++;
                } else if (tok.equals("]")) {
                    depth--;
                }
            }
        }

        private static String unquote(String v) {
            return v.length() >= 2 && (v.startsWith("\"") || v.startsWith("'")) ? v.substring(1, v.length() - 1) : v;
        }

        private static String camel(String name) {
            StringBuilder sb = new StringBuilder();
            boolean upper = true;
            for (char c : name.toCharArray()) {
                if (c == '_') {
                    upper = true;
                } else {
                    sb.append(upper ? Character.toUpperCase(c) : c);
                    upper = false;
                }
            }
            return sb.toString();
        }
    }

    /** 토큰: 식별자(점 포함 가능), 숫자, 따옴표 문자열(따옴표 유지), 기호 한 글자. 주석(//, /* *&#47;)은 건너뛴다 */
    private static final class Tokenizer {
        private final String s;
        private int pos;
        private int line = 1;
        private String peeked;
        private int peekedLine;

        Tokenizer(String s) {
            this.s = s;
        }

        boolean eof() {
            return peek() == null;
        }

        String peek() {
            if (peeked == null) {
                int before = line;
                peeked = read();
                peekedLine = line;
                line = before == line ? line : line;
            }
            return peeked;
        }

        boolean peekIs(String v) {
            return v.equals(peek());
        }

        String next() {
            String tok = peek();
            if (tok == null) {
                throw error("파일이 예상보다 일찍 끝났습니다");
            }
            peeked = null;
            return tok;
        }

        boolean accept(String v) {
            if (v.equals(peek())) {
                peeked = null;
                return true;
            }
            return false;
        }

        void expect(String v) {
            String tok = next();
            if (!tok.equals(v)) {
                throw error("'" + v + "'이(가) 와야 하는데 '" + tok + "'입니다");
            }
        }

        String ident() {
            String tok = next();
            if (!Character.isLetter(tok.charAt(0)) && tok.charAt(0) != '_' || tok.contains(".")) {
                throw error("이름이 와야 하는데 '" + tok + "'입니다");
            }
            return tok;
        }

        String fullIdent() {
            String tok = next();
            if (!Character.isLetter(tok.charAt(0)) && tok.charAt(0) != '_' && tok.charAt(0) != '.') {
                throw error("이름이 와야 하는데 '" + tok + "'입니다");
            }
            return tok;
        }

        String string() {
            String tok = next();
            if (tok.length() < 2 || tok.charAt(0) != '"' && tok.charAt(0) != '\'') {
                throw error("문자열이 와야 하는데 '" + tok + "'입니다");
            }
            return tok.substring(1, tok.length() - 1);
        }

        int integer() {
            String tok = next();
            try {
                if (tok.startsWith("0x") || tok.startsWith("0X")) {
                    return Integer.parseInt(tok.substring(2), 16);
                }
                if (tok.length() > 1 && tok.startsWith("0")) {
                    return Integer.parseInt(tok.substring(1), 8);
                }
                return Integer.parseInt(tok);
            } catch (NumberFormatException e) {
                throw error("정수가 와야 하는데 '" + tok + "'입니다");
            }
        }

        SchemaInvalidException error(String message) {
            return new SchemaInvalidException((peeked != null ? peekedLine : line) + "번째 줄: " + message);
        }

        private String read() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == '\n') {
                    line++;
                    pos++;
                } else if (Character.isWhitespace(c)) {
                    pos++;
                } else if (s.startsWith("//", pos)) {
                    while (pos < s.length() && s.charAt(pos) != '\n') {
                        pos++;
                    }
                } else if (s.startsWith("/*", pos)) {
                    int end = s.indexOf("*/", pos + 2);
                    if (end < 0) {
                        throw new SchemaInvalidException(line + "번째 줄: 닫히지 않은 주석");
                    }
                    for (int i = pos; i < end; i++) {
                        if (s.charAt(i) == '\n') {
                            line++;
                        }
                    }
                    pos = end + 2;
                } else {
                    break;
                }
            }
            if (pos >= s.length()) {
                return null;
            }
            char c = s.charAt(pos);
            int start = pos;
            if (Character.isLetterOrDigit(c) || c == '_' || c == '.') {
                while (pos < s.length() && (Character.isLetterOrDigit(s.charAt(pos)) || s.charAt(pos) == '_'
                        || s.charAt(pos) == '.')) {
                    pos++;
                }
                return s.substring(start, pos);
            }
            if (c == '"' || c == '\'') {
                pos++;
                StringBuilder sb = new StringBuilder().append(c);
                while (pos < s.length() && s.charAt(pos) != c) {
                    if (s.charAt(pos) == '\\' && pos + 1 < s.length()) {
                        pos++;
                    }
                    if (s.charAt(pos) == '\n') {
                        throw new SchemaInvalidException(line + "번째 줄: 닫히지 않은 문자열");
                    }
                    sb.append(s.charAt(pos++));
                }
                if (pos >= s.length()) {
                    throw new SchemaInvalidException(line + "번째 줄: 닫히지 않은 문자열");
                }
                pos++;
                return sb.append(c).toString();
            }
            pos++;
            return String.valueOf(c);
        }
    }
}
