package net.java21.data2flow.ingress.support;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 테스트 전용 RabbitMQ 3.13(운영 s4와 같은 계열, stream 플러그인). AMQP 5672, Stream 5552. 공용 s4 RabbitMQ에는 붙지 않는다.
 * Stream 앞에는 Toxiproxy를 두어 RabbitMQ 끊김·지연을 흉내 낸다(TC-ING-016, TC-ING-017).
 */
public final class RabbitTestBroker {

    public static final int AMQP = 5672;
    public static final int STREAM = 5552;
    private static final Network NETWORK = Network.newNetwork();
    private static GenericContainer<?> rabbit;
    private static ToxiproxyContainer toxiproxy;

    private RabbitTestBroker() {
    }

    public static synchronized GenericContainer<?> rabbit() {
        if (rabbit == null) {
            rabbit = new GenericContainer<>("rabbitmq:3.13-management")
                    .withNetwork(NETWORK).withNetworkAliases("rabbit")
                    .withExposedPorts(AMQP, STREAM)
                    .withCopyToContainer(Transferable.of("[rabbitmq_management,rabbitmq_stream]."),
                            "/etc/rabbitmq/enabled_plugins")
                    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(3)));
            rabbit.start();
        }
        return rabbit;
    }

    public static synchronized ToxiproxyContainer toxiproxy() {
        if (toxiproxy == null) {
            rabbit();
            toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                    .withNetwork(NETWORK).withExposedPorts(8474, 8666, 8667, 8668, 8669, 8670, 8671);
            toxiproxy.start();
        }
        return toxiproxy;
    }

    private static final java.util.Map<String, eu.rekawek.toxiproxy.Proxy> PROXIES = new java.util.HashMap<>();
    private static int nextProxyPort = 8666;

    /**
     * Stream 포트(5552) 앞의 Toxiproxy 프록시(이름마다 하나). 지연·차단으로 confirm이 늦거나 오지 않는 상황을 만든다.
     *
     * @return 프록시와 앱이 접속할 host:port
     */
    public static synchronized StreamProxy streamProxy(String name) {
        try {
            ToxiproxyContainer tp = toxiproxy();
            eu.rekawek.toxiproxy.Proxy proxy = PROXIES.get(name);
            int listen;
            if (proxy == null) {
                listen = nextProxyPort++;
                proxy = new eu.rekawek.toxiproxy.ToxiproxyClient(tp.getHost(), tp.getControlPort())
                        .createProxy(name, "0.0.0.0:" + listen, "rabbit:" + STREAM);
                PROXIES.put(name, proxy);
            } else {
                listen = Integer.parseInt(proxy.getListen().replaceAll(".*:", ""));
            }
            return new StreamProxy(proxy, tp.getHost(), tp.getMappedPort(listen));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Stream Toxiproxy 프록시와 접속 주소 */
    public record StreamProxy(eu.rekawek.toxiproxy.Proxy proxy, String host, int port) {

        /** 응답(confirm) 방향에 지연을 준다 */
        public void latency(long millis) {
            try {
                removeToxics();
                proxy.toxics().latency("confirm-latency", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, millis);
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        public void removeToxics() {
            try {
                for (eu.rekawek.toxiproxy.model.Toxic t : proxy.toxics().getAll()) {
                    t.remove();
                }
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        /** 연결을 끊고 새 연결을 막는다(RabbitMQ 장애 흉내) / 다시 연다 */
        public void enabled(boolean value) {
            try {
                if (value) {
                    proxy.enable();
                } else {
                    proxy.disable();
                }
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static String host() {
        return rabbit().getHost();
    }

    public static int amqpPort() {
        return rabbit().getMappedPort(AMQP);
    }

    public static int streamPort() {
        return rabbit().getMappedPort(STREAM);
    }

    /** ingress 설정: 스트림은 주어진 host:port로 고정 접속(테스트·로컬 터널과 같은 방식) */
    public static Map<String, String> properties(String streamHost, int streamPort) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("spring.rabbitmq.host", host());
        p.put("spring.rabbitmq.port", Integer.toString(amqpPort()));
        p.put("spring.rabbitmq.virtual-host", "/");
        p.put("spring.rabbitmq.username", "guest");
        p.put("spring.rabbitmq.password", "guest");
        p.put("data2flow.ingress.stream.host", streamHost);
        p.put("data2flow.ingress.stream.port", Integer.toString(streamPort));
        p.put("data2flow.ingress.stream.virtual-host", "/");
        p.put("data2flow.ingress.stream.username", "guest");
        p.put("data2flow.ingress.stream.password", "guest");
        p.put("data2flow.ingress.stream.use-configured-address", "true");
        return p;
    }

    public static Map<String, String> properties() {
        return properties(host(), streamPort());
    }

    /** 테스트가 스트림을 읽는 환경(직접 접속) */
    public static Environment environment() {
        String host = host();
        int port = streamPort();
        return Environment.builder().host(host).port(port).username("guest").password("guest")
                .addressResolver(a -> new Address(host, port)).build();
    }
}
