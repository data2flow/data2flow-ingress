package net.java21.data2flow.ingress.signing;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.PlatformBrokerSignature;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SignatureStatus;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-03.03·03.05(TC-DSC-114·TC-DSC-322 ingress 부분, ADR-042): 테스트 Mosquitto의 {@code devices/{deviceKey}/telemetry}를
 * 플랫폼 브로커 소스로 구독하고, 서명 키(API-DSC-72 대역)로 검증한 결과를 {@code data2flow.raw}의 signatureStatus로 싣는다.
 * 발행은 이 테스트 컨테이너에만 한다(공용 브로커에는 발행하지 않음).
 */
class PlatformBrokerSignatureIT extends AbstractIngressAppIT {

    static final long SOURCE = 7;
    static final String DEVICE = "esp32-co2-02";
    static final String TOPIC = "devices/" + DEVICE + "/telemetry";
    static final MessageCodec CODEC = MessageCodec.create();

    static RawStreamReader reader;
    static MqttTestPublisher publisher;

    @Autowired
    RabbitTemplate rabbit;
    @Autowired
    SourceSupervisor supervisor;
    @Autowired
    MeterRegistry meters;

    @BeforeAll
    static void setUp() {
        CORE.sources("pb-1", """
                [{"id":%d,"organizationId":1,"type":"PLATFORM_BROKER","lifecycle":"ACTIVE",
                  "config":{"deviceKeyPattern":"{externalId}"},"secrets":{},"clientId":"data2flow-ingress"}]""".formatted(SOURCE));
        CORE.signingKeys("[]");
        publisher = publisher(TOPIC);
    }

    @AfterAll
    static void tearDown() {
        if (reader != null) {
            reader.close();
        }
        publisher.close();
    }

    /** 같은 앱 컨텍스트를 쓰는 다른 IT가 이 소스를 보지 않게 소스를 빼고 닫힐 때까지 기다린다 */
    @AfterEach
    void removeSource() {
        CORE.sources("pb-empty", "[]");
        CORE.signingKeys("[]");
        configChanged(ConfigChangedMessage.EntityType.SOURCE, SOURCE);
        await().atMost(Duration.ofSeconds(30)).until(() -> supervisor.running().stream()
                .noneMatch(r -> r.definition().id() == SOURCE));
    }

    void configChanged(ConfigChangedMessage.EntityType type, long id) {
        MessageProperties props = new MessageProperties();
        props.setContentType("application/json");
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "",
                new Message(CODEC.write(ConfigChangedMessage.upsert(type, id, 1, 1, Clock.systemUTC())), props));
    }

    RawEnvelope publishAndRead(byte[] payload, String marker) {
        publisher.publish(payload);
        await().atMost(Duration.ofSeconds(30)).until(() -> find(marker).isPresent());
        return find(marker).orElseThrow();
    }

    static Optional<RawEnvelope> find(String marker) {
        return reader.envelopes().stream().filter(e -> e.sourceId() == SOURCE)
                .filter(e -> new String(e.payload(), StandardCharsets.UTF_8).contains(marker)).findFirst();
    }

    static byte[] body(String marker) {
        return ("{\"co2\":812,\"m\":\"" + marker + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    double rejected(String reason) {
        return meters.counter("data2flow.ingest.signature.rejected", "reason", reason).count();
    }

    @Test
    @DisplayName("DSC-03.05 TC-DSC-322 승인 전(키 없음)은 UNSIGNED, 승인 후 자격 변경을 받으면 서명 맞음 VERIFIED·서명 없음·다른 키 INVALID")
    void signatureLifecycle() {
        configChanged(ConfigChangedMessage.EntityType.SOURCE, SOURCE);
        await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                .anyMatch(r -> r.definition().id() == SOURCE
                        && r.session().status().state() == net.java21.data2flow.contracts.connector.ConnectorState.CONNECTED));
        assertThat(supervisor.running().stream().filter(r -> r.definition().id() == SOURCE).findFirst().orElseThrow()
                .config().config().path("url").asString()).startsWith("tcp://");
        reader = new RawStreamReader(RabbitTestBroker.environment());

        String m1 = UUID.randomUUID().toString();
        RawEnvelope pending = publishAndRead(body(m1), m1);
        assertThat(pending.sourceType()).isEqualTo(SourceTypes.PLATFORM_BROKER);
        assertThat(pending.topic()).isEqualTo(TOPIC);
        assertThat(pending.signatureStatus()).isEqualTo(SignatureStatus.UNSIGNED);

        // 승인: core가 서명 키를 발급하고 설정 변경(CREDENTIAL)을 보낸다 → 1초 안에 다시 읽는다
        int before = CORE.signingKeyRequests();
        CORE.signingKeys("""
                [{"credentialId":"21","organizationId":"1","sourceId":"%d","deviceId":"88","deviceKey":"%s","signingKey":"d2f-key-1"}]"""
                .formatted(SOURCE, DEVICE));
        configChanged(ConfigChangedMessage.EntityType.CREDENTIAL, 21);
        await().atMost(Duration.ofSeconds(10)).until(() -> CORE.signingKeyRequests() > before);

        String m2 = UUID.randomUUID().toString();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            String m = m2 + "-" + UUID.randomUUID();
            RawEnvelope signed = publishAndRead(PlatformBrokerSignature.sign("d2f-key-1", body(m)), m);
            assertThat(signed.signatureStatus()).isEqualTo(SignatureStatus.VERIFIED);
            assertThat(signed.payload()).isEqualTo(body(m));
        });

        double missingBefore = rejected("missing");
        String m3 = UUID.randomUUID().toString();
        RawEnvelope unsigned = publishAndRead(body(m3), m3);
        assertThat(unsigned.signatureStatus()).isEqualTo(SignatureStatus.INVALID);
        assertThat(rejected("missing")).isEqualTo(missingBefore + 1);

        String m4 = UUID.randomUUID().toString();
        byte[] forged = PlatformBrokerSignature.sign("another-devices-key", body(m4));
        RawEnvelope invalid = publishAndRead(forged, m4);
        assertThat(invalid.signatureStatus()).isEqualTo(SignatureStatus.INVALID);
        assertThat(invalid.payload()).isEqualTo(forged);
        assertThat(rejected("mismatch")).isGreaterThanOrEqualTo(1);
    }
}
