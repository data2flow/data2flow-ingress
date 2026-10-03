package net.java21.data2flow.ingress;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.message.event.SourceRuntimeReported;
import net.java21.data2flow.contracts.message.event.SourceStatsReported;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.StreamRoutingKeys;
import net.java21.data2flow.contracts.test.message.MessageFixtures;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.support.AbstractIngressAppIT;
import net.java21.data2flow.ingress.support.CoreStub;
import net.java21.data2flow.ingress.support.MqttTestPublisher;
import net.java21.data2flow.ingress.support.RabbitTestBroker;
import net.java21.data2flow.ingress.support.RawStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 수집 경로 전체(앱): core 설정(API-DSC-50) → MQTT 구독 → {@code data2flow.raw} 기록 → 상태 보고. ING-01.01, DSC-01.04, DSC-02.01,
 * DSC-02.03, DSC-02.06, DSC-07.01, DSC-09.02, BR-DSC-01, BR-DSC-05.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IngressCollectionIT extends AbstractIngressAppIT {

    static final long SOURCE = 3;
    static final String APP = "6a1c5b0e-0000-4000-8000-000000000001";
    static final String TOPIC = "application/" + APP + "/device/24e124743d012436/event/up";
    static final String EVENTS_QUEUE = "it.ingress.events";
    static final MessageCodec CODEC = MessageCodec.create();

    static RawStreamReader reader;
    static MqttTestPublisher publisher;

    @Autowired
    RabbitTemplate rabbit;
    @Autowired
    AmqpAdmin admin;
    @Autowired
    SourceSupervisor supervisor;
    @LocalServerPort
    int port;

    @BeforeAll
    static void setUpSource() {
        CORE.sources("1", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", brokerUrl(), "application/+/device/+/event/up", null) + "]");
        publisher = publisher(TOPIC);
    }

    @AfterAll
    static void tearDown() {
        if (reader != null) {
            reader.close();
        }
        publisher.close();
    }

    void eventsQueue() {
        if (admin.getQueueInfo(EVENTS_QUEUE) == null) {
            Queue q = new Queue(EVENTS_QUEUE, false, false, false);
            admin.declareQueue(q);
            admin.declareBinding(BindingBuilder.bind(q).to(new TopicExchange(MessagingNames.EXCHANGE_EVENTS)).with("#"));
        }
    }

    List<DomainEvent<?>> drainEvents() {
        List<DomainEvent<?>> events = new CopyOnWriteArrayList<>();
        Message m;
        while ((m = rabbit.receive(EVENTS_QUEUE, 200)) != null) {
            events.add(CODEC.readEvent(m.getBody()));
        }
        return events;
    }

    @Test
    @Order(1)
    @DisplayName("ING-01.01 TC-ING-001·002 DSC-09.02 ChirpStack 업링크가 RawEnvelope v1로 data2flow.raw에 기록된다(dedupKey=deduplicationId, 헤더, 스키마)")
    void chirpStackUplinkIsWrittenToRawStream() {
        eventsQueue();
        await().atMost(Duration.ofSeconds(60)).until(() -> supervisor.running().stream()
                .anyMatch(r -> r.session().status().state() == net.java21.data2flow.contracts.connector.ConnectorState.CONNECTED));
        assertThat(CORE.lastCaller()).isEqualTo("data2flow-ingress");
        reader = new RawStreamReader(RabbitTestBroker.environment());

        byte[] uplink = MessageFixtures.chirpStackUplinkPayload();
        publisher.publish(uplink);
        await().atMost(Duration.ofSeconds(30)).until(() -> reader.received().stream()
                .anyMatch(r -> r.envelope().topic().equals(TOPIC)));

        RawStreamReader.Received r = reader.received().stream().filter(x -> x.envelope().topic().equals(TOPIC))
                .findFirst().orElseThrow();
        RawEnvelope e = r.envelope();
        assertThat(e.v()).isEqualTo(1);
        assertThat(e.organizationId()).isEqualTo(1);
        assertThat(e.sourceId()).isEqualTo(SOURCE);
        assertThat(e.sourceType()).isEqualTo(SourceTypes.MQTT_SUBSCRIBE);
        assertThat(e.payload()).isEqualTo(uplink);
        assertThat(e.ingressInstance()).isEqualTo("ingress-it-0");
        assertThat(e.dedupKey()).isEqualTo("chirpstack:3f1e9b2a-0c4d-4e5f-8a6b-7c8d9e0f1a2b");
        assertThat(e.virtual()).isFalse();
        MessageSchemas.assertValid(e);
        assertThat(r.headers()).containsEntry("messageId", e.messageId().toString())
                .containsEntry("v", "1").containsEntry("schema", "raw-envelope").containsEntry("organizationId", "1")
                .containsEntry("routingKey", StreamRoutingKeys.raw(SOURCE, TOPIC));
    }

    @Test
    @Order(2)
    @DisplayName("ING-01.01 같은 소스·토픽(같은 기기)의 메시지는 같은 파티션에 순서대로 들어간다(reliability-and-ha.md §2.2)")
    void sameDeviceSamePartitionInOrder() {
        String run = UUID.randomUUID().toString();
        for (int i = 0; i < 20; i++) {
            publisher.publish(("{\"run\":\"" + run + "\",\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8));
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> ofRun(run).size() >= 20);
        List<RawStreamReader.Received> mine = ofRun(run);
        assertThat(mine.stream().map(RawStreamReader.Received::partition).distinct()).hasSize(1);
        List<Integer> seqs = mine.stream().map(x -> seq(x.envelope())).toList();
        assertThat(seqs).containsExactlyElementsOf(java.util.stream.IntStream.range(0, 20).boxed().toList());
    }

    @Test
    @Order(3)
    @DisplayName("DSC-02.01 DSC-02.03 BR-DSC-01 상태(EVT-DSC-02)·통계(EVT-DSC-03)를 보고한다. client-id는 data2flow-ingress-dev-it-0")
    void reportsRuntimeStatsAndCatalog() {
        List<DomainEvent<?>> events = new CopyOnWriteArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            events.addAll(drainEvents());
            return events.stream().anyMatch(ev -> ev.payload() instanceof SourceRuntimeReported)
                    && events.stream().anyMatch(ev -> ev.payload() instanceof SourceStatsReported s
                    && s.counters().getOrDefault("received", 0L) > 0);
        });
        SourceRuntimeReported runtime = events.stream().map(DomainEvent::payload)
                .filter(SourceRuntimeReported.class::isInstance).map(SourceRuntimeReported.class::cast)
                .filter(p -> p.state() == net.java21.data2flow.contracts.connector.ConnectorState.CONNECTED)
                .findFirst().orElseThrow();
        assertThat(runtime.sourceId()).isEqualTo(SOURCE);
        assertThat(runtime.instanceId()).isEqualTo("ingress-it-0");
        assertThat(runtime.clientId()).isEqualTo("data2flow-ingress-dev-it-0");
        assertThat(runtime.connectedSince()).isNotNull();
        events.forEach(MessageSchemas::assertValid);
        SourceStatsReported stats = events.stream().map(DomainEvent::payload).filter(SourceStatsReported.class::isInstance)
                .map(SourceStatsReported.class::cast).filter(s -> s.counters().getOrDefault("received", 0L) > 0)
                .findFirst().orElseThrow();
        assertThat(stats.producer()).isEqualTo(SourceStatsReported.Producer.INGRESS);
        assertThat(stats.counters()).containsKeys("received", "bytes", "reconnects");
    }

    @Test
    @Order(4)
    @DisplayName("DSC-02.06 API-DSC-52 실시간 원본 보기(SSE)로 수신 메시지가 흐른다(토픽 필터)")
    void liveViewStreamsMessages() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port
                + "/internal/ingress/sources/" + SOURCE + "/live?topicFilter=application/%2B/device/%23")).GET().build();
        HttpResponse<java.io.InputStream> res = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        assertThat(res.statusCode()).isEqualTo(200);
        String marker = "live-" + UUID.randomUUID();
        List<String> lines = new CopyOnWriteArrayList<>();
        Thread reader = Thread.ofVirtual().start(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(res.body(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
            } catch (Exception ignored) {
                // 끝
            }
        });
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            publisher.publish(("{\"marker\":\"" + marker + "\"}").getBytes(StandardCharsets.UTF_8));
            assertThat(lines).anyMatch(l -> l.contains(marker));
        });
        assertThat(lines).anyMatch(l -> l.startsWith("event:message"));
        reader.interrupt();
        res.body().close();
    }

    @Test
    @Order(5)
    @DisplayName("DSC-07.01 BR-DSC-05 설정 변경(EVT-DSC-01)으로 일시정지(세션 유지)하면 쌓인 메시지를 재개 후 받는다")
    void pauseKeepsSessionAndResumeReceivesQueued() {
        CORE.sources("2", "[" + CoreStub.mqttSource(SOURCE, "PAUSED", brokerUrl(), "application/+/device/+/event/up", null) + "]");
        sendConfigChanged();
        await().atMost(Duration.ofSeconds(20)).until(() -> supervisor.running().stream()
                .allMatch(r -> r.session().status().state() == net.java21.data2flow.contracts.connector.ConnectorState.DISABLED));
        String run = UUID.randomUUID().toString();
        for (int i = 0; i < 5; i++) {
            publisher.publish(("{\"run\":\"" + run + "\",\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8));
        }
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> ofRun(run).isEmpty());

        CORE.sources("3", "[" + CoreStub.mqttSource(SOURCE, "ACTIVE", brokerUrl(), "application/+/device/+/event/up", null) + "]");
        sendConfigChanged();
        await().atMost(Duration.ofSeconds(30)).until(() -> ofRun(run).size() >= 5);
    }

    @Test
    @Order(6)
    @DisplayName("BR-DSC-05 목록에서 빠진 소스는 연결을 닫는다")
    void removedSourceIsClosed() {
        CORE.sources("4", "[]");
        sendConfigChanged();
        await().atMost(Duration.ofSeconds(30)).until(() -> supervisor.running().isEmpty());
    }

    void sendConfigChanged() {
        ConfigChangedMessage msg = ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SOURCE, SOURCE, 2, 1,
                Clock.systemUTC());
        org.springframework.amqp.core.MessageProperties props = new org.springframework.amqp.core.MessageProperties();
        props.setContentType("application/json");
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "", new Message(CODEC.write(msg), props));
    }

    static List<RawStreamReader.Received> ofRun(String run) {
        return reader.received().stream()
                .filter(x -> new String(x.envelope().payload(), StandardCharsets.UTF_8).contains(run)).toList();
    }

    static int seq(RawEnvelope e) {
        String s = new String(e.payload(), StandardCharsets.UTF_8);
        return Integer.parseInt(s.replaceAll(".*\"seq\":(\\d+).*", "$1"));
    }
}
