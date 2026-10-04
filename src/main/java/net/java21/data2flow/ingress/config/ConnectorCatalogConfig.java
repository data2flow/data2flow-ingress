package net.java21.data2flow.ingress.config;

import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.amqp.Amqp091Connector;
import net.java21.data2flow.ingress.connector.amqp.Amqp10Connector;
import net.java21.data2flow.ingress.connector.bacnet.BacnetIpConnector;
import net.java21.data2flow.ingress.connector.coap.CoapConnector;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.file.S3FileConnector;
import net.java21.data2flow.ingress.connector.gcp.PubSubConnector;
import net.java21.data2flow.ingress.connector.http.HttpPollingConnector;
import net.java21.data2flow.ingress.connector.http.SseConnector;
import net.java21.data2flow.ingress.connector.kafka.KafkaConnector;
import net.java21.data2flow.ingress.connector.modbus.ModbusTcpConnector;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import net.java21.data2flow.ingress.connector.mqttpreset.MqttPresetConnector;
import net.java21.data2flow.ingress.connector.mqttpreset.MqttPresets;
import net.java21.data2flow.ingress.connector.nats.NatsJetStreamConnector;
import net.java21.data2flow.ingress.connector.onem2m.OneM2mConnector;
import net.java21.data2flow.ingress.connector.opcua.OpcUaConnector;
import net.java21.data2flow.ingress.connector.webhook.ReplayGuard;
import net.java21.data2flow.ingress.connector.webhook.WebhookConnector;
import net.java21.data2flow.ingress.connector.webhook.WebhookRouter;
import net.java21.data2flow.ingress.lease.repository.LeaseRepository;
import net.java21.data2flow.ingress.lease.repository.WebhookRequestRepository;
import net.java21.data2flow.ingress.lease.service.JdbcLeaseManager;
import net.java21.data2flow.ingress.lease.service.LeaseManager;
import net.java21.data2flow.ingress.lease.service.LocalLeaseManager;
import net.java21.data2flow.ingress.webhook.service.JdbcReplayGuard;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * 커넥터 카탈로그(DSC-09, connectors.md §2). 빈으로 둔 커넥터를 {@code ConnectorRegistry}가 모아 카탈로그로 보고한다(EVT-DSC-09).
 * 모든 커넥터는 계약 키트(AbstractConnectorContractTest)와 라이선스 검사(ConnectorLicenseTest)를 통과한 것만 둔다(BR-DSC-23).
 */
@Configuration(proxyBeanMethods = false)
public class ConnectorCatalogConfig {

    @Bean
    PollingOptions pollingOptions(IngressProperties properties) {
        return PollingOptions.defaults().withMinPollInterval(properties.polling().minInterval());
    }

    @Bean
    Amqp091Connector amqp091Connector(PollingOptions o, Clock clock) {
        return new Amqp091Connector(o, clock);
    }

    @Bean
    Amqp10Connector amqp10Connector(PollingOptions o, Clock clock) {
        return new Amqp10Connector(o, clock);
    }

    @Bean
    KafkaConnector kafkaConnector(PollingOptions o, Clock clock) {
        return new KafkaConnector(o, clock);
    }

    @Bean
    NatsJetStreamConnector natsJetStreamConnector(PollingOptions o, Clock clock) {
        return new NatsJetStreamConnector(o, clock);
    }

    @Bean
    PubSubConnector pubSubConnector(PollingOptions o, Clock clock) {
        return new PubSubConnector(o, clock);
    }

    @Bean
    HttpPollingConnector httpPollingConnector(PollingOptions o, Clock clock) {
        return new HttpPollingConnector(o, clock);
    }

    @Bean
    SseConnector sseConnector(PollingOptions o, Clock clock) {
        return new SseConnector(o, clock);
    }

    @Bean
    CoapConnector coapConnector(PollingOptions o, Clock clock) {
        return new CoapConnector(o, clock);
    }

    @Bean
    OpcUaConnector opcUaConnector(PollingOptions o, Clock clock) {
        return new OpcUaConnector(o, clock);
    }

    @Bean
    ModbusTcpConnector modbusTcpConnector(PollingOptions o, Clock clock) {
        return new ModbusTcpConnector(o, clock);
    }

    @Bean
    BacnetIpConnector bacnetIpConnector(PollingOptions o, Clock clock) {
        return new BacnetIpConnector(o, clock);
    }

    @Bean
    OneM2mConnector oneM2mConnector(PollingOptions o, Clock clock) {
        return new OneM2mConnector(o, clock);
    }

    @Bean
    S3FileConnector s3FileConnector(PollingOptions o, Clock clock) {
        return new S3FileConnector(o, clock);
    }

    @Bean
    MqttPresetConnector sparkplugConnector(MqttSourceConnector mqtt) {
        return MqttPresets.sparkplug(mqtt);
    }

    @Bean
    MqttPresetConnector theThingsStackConnector(MqttSourceConnector mqtt) {
        return MqttPresets.theThingsStack(mqtt);
    }

    @Bean
    MqttPresetConnector awsIotConnector(MqttSourceConnector mqtt) {
        return MqttPresets.awsIot(mqtt);
    }

    @Bean
    MqttPresetConnector azureIotHubConnector(MqttSourceConnector mqtt, Clock clock) {
        return MqttPresets.azureIotHub(mqtt, clock);
    }

    // ---- Webhook 수신(DSC-01.03) ----

    @Bean
    WebhookRouter webhookRouter() {
        return new WebhookRouter();
    }

    /** 재생 방지: 저장소가 있으면 인스턴스 사이에 공유(PostgreSQL), 없으면 메모리 */
    @Bean
    ReplayGuard replayGuard(ObjectProvider<WebhookRequestRepository> repository) {
        WebhookRequestRepository r = repository.getIfAvailable();
        return r == null ? ReplayGuard.inMemory() : new JdbcReplayGuard(r);
    }

    @Bean
    WebhookConnector webhookConnector(WebhookRouter router, ReplayGuard replay, IngressProperties properties) {
        return new WebhookConnector(router, replay, properties.webhook().writeTimeout());
    }

    // ---- SINGLETON 리스(DSC-09.10) ----

    @Bean
    LeaseManager leaseManager(ObjectProvider<LeaseRepository> repository, IngressProperties properties, Clock clock) {
        LeaseRepository r = repository.getIfAvailable();
        if (r == null) {
            LoggerFactory.getLogger(ConnectorCatalogConfig.class).warn(
                    "ingress 저장소가 없습니다(data2flow.ingress.db.url): 폴링 위치를 메모리에만 두고 이 인스턴스가 늘 리더입니다(로컬 전용)");
            return new LocalLeaseManager();
        }
        return new JdbcLeaseManager(r, properties.instanceId(), properties.lease().ttl(), clock);
    }
}
