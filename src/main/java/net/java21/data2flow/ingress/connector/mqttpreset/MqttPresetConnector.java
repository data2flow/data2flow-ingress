package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import tools.jackson.databind.JsonNode;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * MQTT 위에 얹은 커넥터(Sparkplug B, The Things Stack v3, AWS IoT Core, Azure IoT Hub). 사용자는 그 서비스의 말(애플리케이션 ID, 허브 이름,
 * 엔드포인트)로 설정하고, 이 커넥터가 MQTT 설정({@link MqttSourceConnector}의 평평한 모양)으로 바꿔 같은 무손실 경로(스트림 confirm 뒤
 * PUBACK)를 쓴다. 데이터가 아닌 토픽(Sparkplug STATE·명령)은 기록하지 않고 확인만 한다.
 */
public class MqttPresetConnector implements SourceConnector {

    /** 프리셋 설정 → MQTT 설정 */
    @FunctionalInterface
    public interface Mapper {
        SourceConfig toMqtt(SourceConfig preset);
    }

    private final String key;
    private final String name;
    private final ConnectorCategory category;
    private final Set<AuthMethod> authMethods;
    private final Set<PayloadFormat> formats;
    private final MqttSourceConnector mqtt;
    private final Mapper mapper;
    private final Predicate<String> dataTopic;
    private final JsonNode schema;

    public MqttPresetConnector(String key, String name, ConnectorCategory category, Set<AuthMethod> authMethods,
                               Set<PayloadFormat> formats, MqttSourceConnector mqtt, Mapper mapper, Predicate<String> dataTopic) {
        this.key = key;
        this.name = name;
        this.category = category;
        this.authMethods = authMethods;
        this.formats = formats;
        this.mqtt = mqtt;
        this.mapper = mapper;
        this.dataTopic = dataTopic;
        this.schema = ConnectorSchemas.load(key);
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(key, name, "1.0.0", category, authMethods, formats, AckMode.AFTER_WRITE,
                ScalingMode.DUAL_ACTIVE, false);
    }

    @Override
    public JsonNode configSchema() {
        return schema;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        return mqtt.test(mapper.toMqtt(config));
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        RawSink filtered = envelope -> dataTopic.test(envelope.topic()) ? sink.write(envelope)
                : CompletableFuture.completedFuture(null);
        return mqtt.open(mapper.toMqtt(config), filtered, ctx);
    }

    /** MQTT 설정이 실제로 정하는 확장 방식(BR-DSC-25) */
    public ScalingMode scaling(SourceConfig config) {
        return mqtt.scaling(mapper.toMqtt(config));
    }
}
