package net.java21.data2flow.ingress.connector.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.MqttClientSslConfigBuilder;
import com.hivemq.client.mqtt.MqttClientTransportConfig;
import com.hivemq.client.mqtt.MqttClientTransportConfigBuilder;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.MqttWebSocketConfig;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.lifecycle.MqttDisconnectSource;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.auth.Mqtt3SimpleAuth;
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3Connect;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscription;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAckReturnCode;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictions;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5RetainHandling;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * HiveMQ MQTT 클라이언트(3.1.1·5) 한 번의 연결. 세션이 연결을 다시 맺을 때마다 새로 만든다(재전송은 영속 세션이 받는다).
 *
 * <p><b>구독 전용이다.</b> 이 클래스에는 발행(PUBLISH) 경로가 없다(CLAUDE.md §5: 외부 브로커에는 어떤 토픽에도 발행하지 않는다).
 * 수신은 수동 확인(manual acknowledgement)으로 등록해, 호출하는 쪽이 스트림 confirm 뒤에 {@link Incoming#ack()}를 부를 때만 PUBACK이 나간다.
 */
public final class MqttLink {

    /**
     * 받은 메시지.
     *
     * @param ack QoS 1·2의 확인(PUBACK·PUBREC). QoS 0이면 아무것도 하지 않는다
     */
    public record Incoming(String topic, byte[] payload, int qos, boolean duplicate, Runnable ack) {
    }

    private final MqttSourceSettings settings;
    private final Mqtt5AsyncClient v5;
    private final Mqtt3AsyncClient v3;

    /**
     * @param onMessage      받은 메시지(연결 전에 등록해 영속 세션이 곧바로 보내는 메시지도 받는다)
     * @param onDisconnected 연결이 끊김(사용자가 끊은 것 제외)
     * @param executor       수신 콜백 실행기. 순서를 지키려면 단일 스레드여야 한다
     */
    public MqttLink(MqttSourceSettings settings, String clientId, long connectTimeoutMillis, Consumer<Incoming> onMessage,
                    Consumer<Throwable> onDisconnected, Executor executor) {
        this.settings = settings;
        MqttClientTransportConfig transport = transport(settings, connectTimeoutMillis);
        if (settings.v5()) {
            v5 = MqttClient.builder().useMqttVersion5().identifier(clientId).transportConfig(transport)
                    .addDisconnectedListener(ctx -> {
                        if (ctx.getSource() != MqttDisconnectSource.USER) {
                            onDisconnected.accept(ctx.getCause());
                        }
                    })
                    .buildAsync();
            v3 = null;
            v5.publishes(MqttGlobalPublishFilter.ALL, p -> onMessage.accept(new Incoming(p.getTopic().toString(),
                    p.getPayloadAsBytes(), p.getQos().getCode(), false, ackOf(p.getQos(), p::acknowledge))), executor, true);
        } else {
            v3 = MqttClient.builder().useMqttVersion3().identifier(clientId).transportConfig(transport)
                    .addDisconnectedListener(ctx -> {
                        if (ctx.getSource() != MqttDisconnectSource.USER) {
                            onDisconnected.accept(ctx.getCause());
                        }
                    })
                    .buildAsync();
            v5 = null;
            v3.publishes(MqttGlobalPublishFilter.ALL, p -> onMessage.accept(new Incoming(p.getTopic().toString(),
                    p.getPayloadAsBytes(), p.getQos().getCode(), false, ackOf(p.getQos(), p::acknowledge))), executor, true);
        }
    }

    private static Runnable ackOf(MqttQos qos, Runnable acknowledge) {
        return qos == MqttQos.AT_MOST_ONCE ? () -> { } : acknowledge;
    }

    /**
     * CONNECT → CONNACK.
     *
     * @param cleanStart       세션을 새로 시작할지(연결 테스트는 true)
     * @param sessionExpirySec 영속 세션 만료(MQTT 5)
     * @param receiveMaximum   MQTT 5 동시 미확인 한도(역압)
     */
    public CompletableFuture<Void> connect(boolean cleanStart, long sessionExpirySec, int receiveMaximum) {
        byte[] password = settings.password() == null ? null : settings.password().reveal().getBytes(StandardCharsets.UTF_8);
        if (v5 != null) {
            var b = Mqtt5Connect.builder().cleanStart(cleanStart).sessionExpiryInterval(cleanStart ? 0 : sessionExpirySec)
                    .keepAlive(settings.keepAliveSec())
                    .restrictions(Mqtt5ConnectRestrictions.builder().receiveMaximum(receiveMaximum).build());
            if (settings.username() != null || password != null) {
                var auth = Mqtt5SimpleAuth.builder();
                Mqtt5SimpleAuth simple = settings.username() != null
                        ? (password != null ? auth.username(settings.username()).password(password).build()
                        : auth.username(settings.username()).build())
                        : auth.password(password).build();
                b.simpleAuth(simple);
            }
            return v5.connect(b.build()).thenApply(ack -> null);
        }
        var b = Mqtt3Connect.builder().cleanSession(cleanStart).keepAlive(settings.keepAliveSec());
        if (settings.username() != null) {
            var auth = Mqtt3SimpleAuth.builder().username(settings.username());
            b.simpleAuth(password != null ? auth.password(password).build() : auth.build());
        }
        return v3.connect(b.build()).thenApply(ack -> null);
    }

    /** SUBSCRIBE → SUBACK. 브로커가 거부한 토픽이 있으면 실패 */
    public CompletableFuture<Void> subscribe() {
        List<MqttSourceSettings.Subscription> topics = settings.topics();
        if (v5 != null) {
            List<Mqtt5Subscription> subs = topics.stream().map(s -> {
                var sub = Mqtt5Subscription.builder().topicFilter(s.topic()).qos(qos(s.qos()));
                if (!s.topic().startsWith("$share/")) {
                    sub.retainHandling(retainHandling(settings.retainHandling()));
                }
                return (Mqtt5Subscription) sub.build();
            }).toList();
            return v5.subscribe(Mqtt5Subscribe.builder().addSubscriptions(subs).build()).thenAccept(ack -> {
                for (Mqtt5SubAckReasonCode code : ack.getReasonCodes()) {
                    if (code.isError()) {
                        throw new SubscribeRejectedException("SUBACK " + code);
                    }
                }
            });
        }
        List<Mqtt3Subscription> subs = topics.stream()
                .map(s -> Mqtt3Subscription.builder().topicFilter(s.topic()).qos(qos(s.qos())).build()).toList();
        return v3.subscribe(Mqtt3Subscribe.builder().addSubscriptions(subs).build()).thenAccept(ack -> {
            for (Mqtt3SubAckReturnCode code : ack.getReturnCodes()) {
                if (code.isError()) {
                    throw new SubscribeRejectedException("SUBACK " + code);
                }
            }
        });
    }

    /** DISCONNECT. 영속 세션은 브로커에 남는다(clean start 없이 끊기, DSC domain-model §3.1) */
    public CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> f = v5 != null ? v5.disconnect() : v3.disconnect();
        return f.exceptionally(e -> null);
    }

    private static MqttQos qos(int code) {
        return MqttQos.fromCode(code);
    }

    private static Mqtt5RetainHandling retainHandling(String value) {
        return switch (value == null ? "SEND" : value) {
            case "DO_NOT_SEND" -> Mqtt5RetainHandling.DO_NOT_SEND;
            case "SEND_IF_SUBSCRIPTION_DOES_NOT_EXIST", "SEND_IF_NEW" -> Mqtt5RetainHandling.SEND_IF_SUBSCRIPTION_DOES_NOT_EXIST;
            default -> Mqtt5RetainHandling.SEND;
        };
    }

    static MqttClientTransportConfig transport(MqttSourceSettings s, long connectTimeoutMillis) {
        MqttClientTransportConfigBuilder b = MqttClientTransportConfig.builder().serverHost(s.host()).serverPort(s.port())
                .socketConnectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
                .mqttConnectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        if (s.tls()) {
            b.sslConfig(sslConfig(s));
        }
        if (s.webSocket()) {
            b.webSocketConfig(MqttWebSocketConfig.builder().serverPath(s.path()).subprotocol("mqtt")
                    .handshakeTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
                    .httpHeaders(s.headers()).build());
        }
        return b.build();
    }

    static MqttClientSslConfig sslConfig(MqttSourceSettings s) {
        try {
            MqttClientSslConfigBuilder b = MqttClientSslConfig.builder().protocols(TlsSupport.PROTOCOLS)
                    .trustManagerFactory(TlsSupport.trustManagers(s.caPem(), s.tlsInsecure()));
            var kmf = TlsSupport.keyManagers(s.clientCertPem(), s.clientKeyPem());
            if (kmf != null) {
                b.keyManagerFactory(kmf);
            }
            if (s.tlsInsecure()) {
                b.hostnameVerifier((host, session) -> true);
            }
            return b.build();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TLS 설정을 만들 수 없습니다: " + e.getMessage(), e);
        }
    }

    /** 브로커가 구독을 거부했다(권한 없음 등) */
    public static final class SubscribeRejectedException extends RuntimeException {
        public SubscribeRejectedException(String message) {
            super(message);
        }
    }
}
