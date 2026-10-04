package net.java21.data2flow.ingress;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import net.java21.data2flow.contracts.test.arch.Data2flowArchRules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * 공통 규칙(design/testing/backend.md §6): Thread.sleep 금지, 시스템 시계 직접 호출 금지, 리포지토리 조회는 조직 조건(ADR-052 저장소).
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

    /** BR-DSC-25·DSC-09.13: M5 커넥터도 받기만 한다(보내기는 M7 ACT 드라이버가 승인 뒤). 발행·쓰기 API를 부르는 운영 코드가 없어야 한다 */
    @ArchTest
    static final ArchRule connectorsAreReceiveOnly = noClasses().should().callMethodWhere(
                    com.tngtech.archunit.base.DescribedPredicate.describe("외부 시스템 발행·쓰기", call -> {
                        String owner = call.getTarget().getOwner().getName();
                        String name = call.getTarget().getName();
                        return owner.equals("com.rabbitmq.client.Channel") && name.equals("basicPublish")
                                || owner.startsWith("org.apache.kafka.clients.producer.")
                                || owner.startsWith("io.nats.client.") && name.startsWith("publish")
                                || owner.startsWith("org.apache.qpid.protonj2.client.") && name.equals("openSender")
                                || owner.startsWith("org.eclipse.milo.opcua.sdk.client.") && name.startsWith("write")
                                || owner.startsWith("com.ghgande.j2mod.modbus.facade.") && name.startsWith("write")
                                || owner.startsWith("software.amazon.awssdk.services.s3.") && (name.startsWith("put")
                                || name.startsWith("delete") || name.startsWith("copy"))
                                || owner.startsWith("org.eclipse.californium.core.CoapClient")
                                && (name.startsWith("post") || name.startsWith("put") || name.startsWith("delete"));
                    }))
            .because("CLAUDE.md §5·DSC-09.13: 수집 커넥터는 구독·읽기만 한다. 제어 송신은 M7에서 승인 뒤에 연다");

    @ArchTest
    static final ArchRule repositoriesAreOrganizationScoped = Data2flowArchRules.REPOSITORY_QUERIES_ARE_ORGANIZATION_SCOPED;
}
