package net.java21.data2flow.ingress.connector;

import net.java21.data2flow.contracts.license.LicensePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSC-09.14 TC-DSC-318 AT-DSC-18.2 BR-DSC-23 ADR-018: ingress 실행 의존성(커넥터 라이브러리 포함)의 SBOM(CycloneDX, {@code target/bom.json},
 * 빌드의 process-test-classes 단계에서 만든다)에 금지·미확인 라이선스가 0건이다. 한 의존성에 라이선스가 여럿이면 고를 수 있는 것(OR)으로 본다.
 */
class ConnectorLicenseTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** POM의 비표준 이름 → SPDX(검토함) */
    private static final Map<String, String> NAMES = Map.of(
            "AL 2.0", "Apache-2.0",
            "ASL 2.0", "Apache-2.0",
            "GPL v2", "GPL-2.0-only",
            "Bouncy Castle Licence", "MIT",                      // Bouncy Castle 라이선스는 MIT 문구와 같다
            "Eclipse Distribution License - Version 1.0", "EDL-1.0",
            "GNU Lesser GPL, Version 3", "LGPL-3.0",
            "GPL-2.0-with-classpath-exception", "GPL-2.0 WITH Classpath-exception-2.0");

    @Test
    @DisplayName("DSC-09.14 TC-DSC-318 커넥터 라이브러리를 포함한 실행 의존성에 금지·미확인 라이선스가 0건")
    void noForbiddenLicenses() throws Exception {
        Path bom = Path.of("target", "bom.json");
        assertThat(bom).as("SBOM이 없습니다. ./mvnw process-test-classes로 만든다(cyclonedx-maven-plugin)").exists();
        JsonNode root = JSON.readTree(Files.readString(bom));
        Map<String, String> licenses = new TreeMap<>();
        for (JsonNode c : root.path("components")) {
            String ga = c.path("group").asString("") + ":" + c.path("name").asString("");
            if (ga.startsWith("net.java21.data2flow:")) {
                continue;   // 우리 라이브러리
            }
            List<String> ids = new ArrayList<>();
            for (JsonNode l : c.path("licenses")) {
                String id = l.has("expression") ? l.path("expression").asString()
                        : l.path("license").has("id") ? l.path("license").path("id").asString()
                        : l.path("license").path("name").asString("");
                if (!id.isBlank()) {
                    ids.add(NAMES.getOrDefault(id, id));
                }
            }
            licenses.put(ga, ids.isEmpty() ? "" : "(" + String.join(" OR ", ids) + ")");
        }
        assertThat(licenses).as("의존성 수").hasSizeGreaterThan(50);
        assertThat(licenses).containsKeys("org.eclipse.milo:milo-sdk-client", "org.apache.kafka:kafka-clients",
                "io.nats:jnats", "org.eclipse.californium:californium-core", "com.ghgande:j2mod");
        assertThat(LicensePolicy.violations(licenses)).as("금지·미확인 라이선스(ADR-018)").isEmpty();
    }

    @Test
    @DisplayName("DSC-09.14 AT-DSC-18.2 BACnet4J(GPL-3.0)를 넣으면 금지로 판정된다(그래서 BACnet은 직접 구현)")
    void gplWouldFail() {
        assertThat(LicensePolicy.violations(Map.of("com.infiniteautomation:bacnet4j", "GPL-3.0-only")))
                .containsEntry("com.infiniteautomation:bacnet4j", LicensePolicy.Verdict.FORBIDDEN);
        assertThat(LicensePolicy.classify("(EPL-2.0 OR Eclipse Distribution License - Version 1.0)")).isNotEqualTo(
                LicensePolicy.Verdict.FORBIDDEN);
    }
}
