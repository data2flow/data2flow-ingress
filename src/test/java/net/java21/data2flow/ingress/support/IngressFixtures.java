package net.java21.data2flow.ingress.support;

import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.source.dto.SourceDefinition;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** 단위 테스트용 설정·소스 빌더 */
public final class IngressFixtures {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private IngressFixtures() {
    }

    public static IngressProperties props(String env, String developer, List<Long> orgs, List<String> denied) {
        return props(env, developer, orgs, denied, Map.of());
    }

    public static IngressProperties props(String env, String developer, List<Long> orgs, List<String> denied,
                                          Map<String, String> credentials) {
        return props(env, developer, orgs, denied, credentials,
                new IngressProperties.PlatformBroker("wss://iot-data.java21.net/mqtt", List.of("devices/+/telemetry"), "5.0", null, null));
    }

    public static IngressProperties props(String env, String developer, List<Long> orgs, List<String> denied,
                                          Map<String, String> credentials, IngressProperties.PlatformBroker platformBroker) {
        return new IngressProperties("data2flow-ingress-1", -1, env, developer, "http://core", Duration.ofMinutes(5),
                Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofSeconds(20), true,
                new IngressProperties.SourceFilter(orgs, denied), credentials,
                new IngressProperties.Mqtt("data2flow-ingress", Duration.ofSeconds(1), Duration.ofSeconds(60), 5,
                        Duration.ofMinutes(5), Duration.ofSeconds(10), 100, 524288),
                new IngressProperties.Stream("h", 5552, "v", "u", "p", false, true, 100),
                new IngressProperties.ConnectionTest(Duration.ofSeconds(15), Duration.ofSeconds(30), 3),
                new IngressProperties.Live(10, 20, Duration.ofMinutes(30)),
                platformBroker,
                new IngressProperties.Signing(Duration.ofSeconds(30)),
                new IngressProperties.Db(null, null, null, "validate", 4),
                new IngressProperties.Lease(Duration.ofSeconds(30), Duration.ofSeconds(10)),
                new IngressProperties.Polling(Duration.ofSeconds(10)),
                new IngressProperties.Webhook(Duration.ofSeconds(30)));
    }

    public static SourceDefinition source(long id, String lifecycle, String url, String topic) {
        JsonNode config = JSON.readTree("{\"url\":\"" + url + "\",\"topics\":[{\"topic\":\"" + topic + "\",\"qos\":1}]}");
        return new SourceDefinition(id, 1, SourceTypes.MQTT_SUBSCRIBE, lifecycle, null, config,
                Map.of("PASSWORD", Secret.of("p")), null);
    }
}
