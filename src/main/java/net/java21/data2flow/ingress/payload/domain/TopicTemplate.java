package net.java21.data2flow.ingress.payload.domain;

import net.java21.data2flow.contracts.message.IngressStatus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 토픽·경로 템플릿(DSC-09.08, BR-DSC-28). 예: {@code site/{site}/room/{room}/{deviceId}/{metric}}.
 *
 * <p>문법({@code /}로 나눈 단계마다):
 * <ul>
 *   <li>{@code {이름}}: 한 단계 전체 또는 일부({@code dev-{id}}). 이름은 영문자로 시작하는 영문·숫자·밑줄, 템플릿 안에서 한 번만.</li>
 *   <li>{@code {이름#}}: 마지막 단계에만. 남은 단계 전체({@code a/b/c})를 담는다(0단계도 된다).</li>
 *   <li>{@code +}(단계 전체): 아무 한 단계, {@code #}(마지막 단계 전체): 남은 단계 전부 — 값은 담지 않는다(MQTT 와일드카드와 같은 뜻).</li>
 *   <li>그 밖의 글자는 그대로 비교한다. 단계 안의 {@code +}·{@code #}(예: {@code a+b})와 정규식 특수문자는 글자 그대로이고,
 *       {@code \{}·{@code \}}·{@code \+}·{@code \#}로 쓰면 언제나 글자다.</li>
 * </ul>
 * 뽑은 값 외에 예약 키를 더한다: {@code externalId}(변수 externalId·deviceId·deviceKey·devEui·device 중 처음 것), {@code metric},
 * {@code spaceHint}(site·building·floor·room·space·zone·area 변수를 템플릿 순서대로 {@code /}로 이은 것, AT-DSC-16.4의 {@code a/301}).
 */
public final class TopicTemplate {

    public static final int MAX_LENGTH = 256;
    private static final Pattern NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");
    private static final List<String> DEVICE_VARS = List.of("externalId", "deviceId", "deviceKey", "devEui", "device");
    private static final Set<String> SPACE_VARS = Set.of("site", "building", "floor", "room", "space", "zone", "area");

    private final String source;
    private final Pattern pattern;
    private final List<String> names = new ArrayList<>();
    private final String filter;

    private TopicTemplate(String source) {
        this.source = source;
        StringBuilder regex = new StringBuilder("^");
        StringJoiner mqtt = new StringJoiner("/");
        String[] segments = source.split("/", -1);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < segments.length; i++) {
            String seg = segments[i];
            boolean last = i == segments.length - 1;
            if (i > 0) {
                regex.append('/');
            }
            if (seg.equals("+")) {
                regex.append("[^/]*");
                mqtt.add("+");
                continue;
            }
            if (seg.equals("#") || seg.matches("\\{[A-Za-z][A-Za-z0-9_]*#}")) {
                if (!last) {
                    throw new IllegalArgumentException("'#'·'{이름#}'은 마지막 단계에만 쓸 수 있습니다");
                }
                if (i > 0) {
                    regex.setLength(regex.length() - 1);   // a/# 는 a 와 a/x/y 둘 다 맞는다(MQTT 규칙)
                    regex.append("(?:/");
                }
                if (seg.equals("#")) {
                    regex.append(".*");
                } else {
                    String name = seg.substring(1, seg.length() - 2);
                    addName(name, seen);
                    regex.append("(?<").append(group(names.size() - 1)).append(">.*)");
                }
                if (i > 0) {
                    regex.append(")?");
                }
                mqtt.add("#");
                continue;
            }
            boolean hasVar = false;
            StringBuilder literal = new StringBuilder();
            int p = 0;
            while (p < seg.length()) {
                char c = seg.charAt(p);
                if (c == '\\' && p + 1 < seg.length() && "{}+#\\".indexOf(seg.charAt(p + 1)) >= 0) {
                    literal.append(seg.charAt(p + 1));
                    p += 2;
                } else if (c == '{') {
                    int end = seg.indexOf('}', p);
                    if (end < 0) {
                        throw new IllegalArgumentException("닫히지 않은 '{'가 있습니다: " + seg);
                    }
                    String name = seg.substring(p + 1, end);
                    if (literal.isEmpty() && hasVar && p > 0 && seg.charAt(p - 1) == '}') {
                        throw new IllegalArgumentException("변수 두 개를 붙여 쓸 수 없습니다: " + seg);
                    }
                    flush(regex, literal);
                    addName(name, seen);
                    regex.append("(?<").append(group(names.size() - 1)).append(">[^/]+?)");
                    hasVar = true;
                    p = end + 1;
                } else if (c == '}') {
                    throw new IllegalArgumentException("짝이 없는 '}'가 있습니다: " + seg);
                } else {
                    literal.append(c);
                    p++;
                }
            }
            flush(regex, literal);
            mqtt.add(hasVar ? "+" : seg.replace("\\", ""));
        }
        regex.append('$');
        this.pattern = Pattern.compile(regex.toString());
        this.filter = mqtt.toString();
    }

    /**
     * @throws IllegalArgumentException 문법 오류(SOURCE_CONFIG_INVALID {@code topicTemplate})
     */
    public static TopicTemplate compile(String template) {
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("템플릿이 비어 있습니다");
        }
        if (template.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("템플릿은 " + MAX_LENGTH + "자 이하여야 합니다");
        }
        return new TopicTemplate(template.strip());
    }

    private void addName(String name, Set<String> seen) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("변수 이름이 올바르지 않습니다: {" + name + "}");
        }
        if (!seen.add(name)) {
            throw new IllegalArgumentException("변수 이름이 두 번 나옵니다: {" + name + "}");
        }
        names.add(name);
    }

    private static void flush(StringBuilder regex, StringBuilder literal) {
        if (!literal.isEmpty()) {
            regex.append(Pattern.quote(literal.toString()));
            literal.setLength(0);
        }
    }

    private static String group(int index) {
        return "v" + index;
    }

    /**
     * @return 맞으면 변수 값 + 예약 키(externalId·metric·spaceHint, 해당 변수가 있을 때만). 맞지 않으면 빈 값(UNMATCHED_TOPIC)
     */
    public Optional<Map<String, String>> match(String topic) {
        if (topic == null) {
            return Optional.empty();
        }
        Matcher m = pattern.matcher(topic);
        if (!m.matches()) {
            return Optional.empty();
        }
        Map<String, String> values = new LinkedHashMap<>();
        StringJoiner space = new StringJoiner("/");
        for (int i = 0; i < names.size(); i++) {
            String v = m.group(group(i));
            String name = names.get(i);
            values.put(name, v == null ? "" : v);
            if (SPACE_VARS.contains(name) && v != null && !v.isEmpty()) {
                space.add(v);
            }
        }
        for (String d : DEVICE_VARS) {
            String v = values.get(d);
            if (v != null && !v.isEmpty()) {
                values.putIfAbsent(IngressStatus.ATTR_EXTERNAL_ID, v);
                break;
            }
        }
        if (space.length() > 0) {
            values.putIfAbsent(IngressStatus.ATTR_SPACE_HINT, space.toString());
        }
        return Optional.of(values);
    }

    /** 변수 이름(템플릿 순서) */
    public List<String> variables() {
        return List.copyOf(names);
    }

    /** 같은 범위를 받는 MQTT 구독 필터(변수 단계는 {@code +}, 남은 단계는 {@code #}). 등록 화면 미리보기용 */
    public String subscriptionFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return source;
    }
}
