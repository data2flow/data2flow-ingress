package net.java21.data2flow.ingress.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connectiontest.service.ConnectionTestService;
import net.java21.data2flow.ingress.connector.mqtt.MqttConnectorOptions;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import net.java21.data2flow.ingress.connector.service.ConnectorRegistry;
import net.java21.data2flow.ingress.live.service.LiveTap;
import net.java21.data2flow.ingress.signing.service.PayloadSignatureVerifier;
import net.java21.data2flow.ingress.signing.service.SigningKeyCache;
import net.java21.data2flow.ingress.signing.service.SigningKeyClient;
import net.java21.data2flow.ingress.source.event.ConfigChangedListener;
import net.java21.data2flow.ingress.source.event.SourceEventPublisher;
import net.java21.data2flow.ingress.source.event.SourceStatusReporter;
import net.java21.data2flow.ingress.source.service.CoreSourceClient;
import net.java21.data2flow.ingress.source.service.RuntimeConfigSync;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import net.java21.data2flow.ingress.stream.service.RawStreamWriter;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AsyncConsumerStartedEvent;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.List;

/**
 * ingress 구성. 수집 경로(커넥터 → {@code data2flow.raw}), core 설정 동기화, 상태 보고, 설정 변경 수신, 연결 테스트, 실시간 보기를 잇는다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(IngressProperties.class)
public class IngressConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    MessageCodec messageCodec() {
        return MessageCodec.create();
    }

    /** OPS-02.03: Micrometer Tracing이 있으면 traceparent를 스트림 헤더에 싣는다. 없으면 noop(동작은 같음) */
    @Bean
    MessageTracing messageTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        return t == null || p == null ? MessageTracing.noop() : new MessageTracing(t, p);
    }

    @Bean(destroyMethod = "close")
    MqttSourceConnector mqttSourceConnector(IngressProperties properties, Clock clock) {
        return new MqttSourceConnector(MqttConnectorOptions.from(properties), clock);
    }

    @Bean
    ConnectorRegistry connectorRegistry(List<SourceConnector> connectors) {
        return new ConnectorRegistry(connectors);
    }

    @Bean
    RawStreamWriter rawStreamWriter(IngressProperties properties, MessageCodec codec, MessageTracing tracing,
                                    MeterRegistry meters) {
        return new RawStreamWriter(properties.stream(), properties.mqtt().confirmTimeout(), SuperStreamSpec.RAW, codec,
                tracing, meters);
    }

    /** readiness: data2flow.raw에 기록할 수 있어야 확인(ACK)할 수 있다(reliability-and-ha.md §4.2) */
    @Bean
    HealthIndicator rawStreamHealthIndicator(RawStreamWriter writer) {
        return () -> writer.isReady()
                ? Health.up().withDetail("unconfirmed", writer.unconfirmed()).build()
                : Health.down().withDetail("error", String.valueOf(writer.lastError())).build();
    }

    /** 운영 확인용: 소스별 연결 상태(관리 포트 8081 /actuator/health). readiness에는 넣지 않는다(외부 브로커 장애로 파드를 빼지 않음) */
    @Bean
    HealthIndicator sourcesHealthIndicator(SourceSupervisor supervisor) {
        return () -> {
            Health.Builder b = Health.up();
            supervisor.running().forEach(r -> b.withDetail(Long.toString(r.definition().id()),
                    java.util.Map.of("state", r.session().status().state().name(),
                            "clientId", String.valueOf(r.config().clientId()),
                            "received", r.session().status().received())));
            return b.build();
        };
    }

    @Bean
    SourceEventPublisher sourceEventPublisher(RabbitTemplate rabbitTemplate, MessageCodec codec, MeterRegistry meters) {
        return new SourceEventPublisher(rabbitTemplate, codec, meters);
    }

    @Bean(destroyMethod = "close")
    SourceStatusReporter sourceStatusReporter(SourceEventPublisher events, IngressProperties properties,
                                              ConnectorRegistry registry, Clock clock,
                                              @Value("${data2flow.ingress.catalog-organization-id:1}") long catalogOrg) {
        return new SourceStatusReporter(events, properties, registry, clock, catalogOrg);
    }

    @Bean
    LiveTap liveTap(IngressProperties properties) {
        return new LiveTap(properties.live());
    }

    /** 플랫폼 브로커 기기 서명 키(API-DSC-72, ADR-042) */
    @Bean
    SigningKeyClient signingKeyClient(RestClient.Builder builder, ObjectProvider<JsonMapper> json, IngressProperties properties) {
        return new SigningKeyClient(builder, json.getIfAvailable(MessageCodec::newMapper), properties);
    }

    @Bean(destroyMethod = "close")
    SigningKeyCache signingKeyCache(SigningKeyClient client, IngressProperties properties, Clock clock) {
        return new SigningKeyCache(client, properties.signing().refreshInterval(), clock);
    }

    @Bean
    PayloadSignatureVerifier payloadSignatureVerifier(SigningKeyCache keys, MeterRegistry meters) {
        return new PayloadSignatureVerifier(keys, meters);
    }

    @Bean
    SourceSupervisor sourceSupervisor(IngressProperties properties, ConnectorRegistry registry, RawStreamWriter writer,
                                      SourceStatusReporter reporter, LiveTap liveTap, MeterRegistry meters, Clock clock,
                                      PayloadSignatureVerifier verifier,
                                      net.java21.data2flow.ingress.lease.service.LeaseManager leaseManager) {
        SourceSupervisor.ReceivedListener received = (RawEnvelope e) -> {
            liveTap.onReceived(e);
            reporter.onReceived(e);
        };
        SourceSupervisor supervisor = new SourceSupervisor(properties, registry, writer, reporter, received, meters, clock,
                verifier);
        supervisor.useLeaseManager(leaseManager);
        reporter.attach(supervisor);
        return supervisor;
    }

    @Bean
    CoreSourceClient coreSourceClient(RestClient.Builder builder, ObjectProvider<JsonMapper> json,
                                      IngressProperties properties) {
        return new CoreSourceClient(builder, json.getIfAvailable(MessageCodec::newMapper), properties);
    }

    @Bean(destroyMethod = "close")
    RuntimeConfigSync runtimeConfigSync(CoreSourceClient core, SourceSupervisor supervisor, RawStreamWriter writer,
                                        IngressProperties properties) {
        return new RuntimeConfigSync(core, supervisor, writer, properties);
    }

    @Bean
    ConnectionTestService connectionTestService(ConnectorRegistry registry, IngressProperties properties) {
        return new ConnectionTestService(registry, properties.connectionTest());
    }

    // ---- RabbitMQ AMQP: 설정 변경 수신(fanout → 인스턴스별 임시 큐), 이벤트 exchange ----

    @Bean
    FanoutExchange configExchange() {
        return new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false);
    }

    @Bean
    TopicExchange eventsExchange() {
        return new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false);
    }

    @Bean
    AnonymousQueue ingressConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("ingress.config."));
    }

    @Bean
    Binding ingressConfigBinding(AnonymousQueue ingressConfigQueue, FanoutExchange configExchange) {
        return BindingBuilder.bind(ingressConfigQueue).to(configExchange);
    }

    @Bean
    ConfigChangedListener configChangedListener(MessageCodec codec, RuntimeConfigSync sync, SigningKeyCache keys) {
        return new ConfigChangedListener(codec, sync, keys::requestRefresh);
    }

    @Bean
    SimpleMessageListenerContainer configChangedContainer(ConnectionFactory connectionFactory,
                                                          AnonymousQueue ingressConfigQueue,
                                                          ConfigChangedListener listener, IngressProperties properties) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(ingressConfigQueue);
        container.setMessageListener(listener::onMessage);
        container.setAutoStartup(properties.autoStart());
        container.setMissingQueuesFatal(false);
        return container;
    }

    /** 시작: 상태 보고와 core 설정 동기화(스트림이 준비되면 첫 설정을 읽는다) */
    @Bean
    ApplicationStartup ingressStartup(IngressProperties properties, SourceStatusReporter reporter,
                                      RuntimeConfigSync sync, ConfigChangedListener listener, SigningKeyCache keys) {
        return new ApplicationStartup(properties, reporter, sync, listener, keys);
    }

    /** 시작 이벤트 처리 */
    public static final class ApplicationStartup {
        private final IngressProperties properties;
        private final SourceStatusReporter reporter;
        private final RuntimeConfigSync sync;
        private final ConfigChangedListener listener;
        private final SigningKeyCache keys;

        ApplicationStartup(IngressProperties properties, SourceStatusReporter reporter, RuntimeConfigSync sync,
                           ConfigChangedListener listener, SigningKeyCache keys) {
            this.keys = keys;
            this.properties = properties;
            this.reporter = reporter;
            this.sync = sync;
            this.listener = listener;
        }

        @EventListener
        public void onReady(ApplicationReadyEvent event) {
            if (properties.autoStart()) {
                keys.start();
                reporter.start();
                sync.start();
            }
        }

        @EventListener
        public void onConsumerStarted(AsyncConsumerStartedEvent event) {
            listener.onConsumerStarted();
        }
    }
}
