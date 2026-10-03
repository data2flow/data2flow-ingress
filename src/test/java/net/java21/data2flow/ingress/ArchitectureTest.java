package net.java21.data2flow.ingress;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import net.java21.data2flow.contracts.test.arch.Data2flowArchRules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * 공통 규칙(design/testing/backend.md §6): Thread.sleep 금지, 시스템 시계 직접 호출 금지. ingress는 DB가 없어 리포지토리 규칙은 해당 없음.
 * 그리고 CLAUDE.md §5: 운영 코드는 외부 MQTT 브로커에 발행하지 않는다 — HiveMQ 발행 API를 부르는 클래스가 없어야 한다.
 */
@AnalyzeClasses(packages = "net.java21.data2flow.ingress", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule noSleep = Data2flowArchRules.NO_THREAD_SLEEP;

    @ArchTest
    static final ArchRule noSystemClock = Data2flowArchRules.NO_SYSTEM_CLOCK;

    @ArchTest
    static final ArchRule subscribeOnly = noClasses().should().callMethodWhere(
                    com.tngtech.archunit.base.DescribedPredicate.describe("MQTT publish", call ->
                            call.getTarget().getOwner().getPackageName().startsWith("com.hivemq.client")
                                    && (call.getTarget().getName().equals("publish")
                                    || call.getTarget().getName().equals("publishWith"))))
            .because("CLAUDE.md §5: 외부 브로커(iot-data.java21.net)에는 어떤 토픽에도 발행하지 않는다(구독만)");
}
