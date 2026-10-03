package net.java21.data2flow.ingress.support;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
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
                    .withNetwork(NETWORK);
            toxiproxy.start();
        }
        return toxiproxy;
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
