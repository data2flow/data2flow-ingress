package net.java21.data2flow.ingress.connector.file;

import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.PollCursorStore;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.test.connector.AbstractConnectorContractTest;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSC-09.09 TC-DSC-327 BR-DSC-24: 파일 가져오기(S3 호환) 커넥터가 S3 호환 서버 컨테이너(adobe/s3mock, Apache-2.0)를 상대로 계약 키트 전체
 * (1,000줄 무손실, 기록 전 위치 저장 금지, 기록 실패 시 같은 줄부터, 재시작 후 이어 읽기·중복 없음)를 통과한다. 보낸 묶음 하나 = jsonl 파일 하나.
 * 확인 수 = 저장된 위치(다 읽은 파일의 줄 수 + 읽는 중인 파일의 줄 번호).
 */
class S3FileConnectorContractIT extends AbstractConnectorContractTest {

    static final GenericContainer<?> S3MOCK = new GenericContainer<>("adobe/s3mock:3.12.0").withExposedPorts(9090)
            .withEnv("initialBuckets", "kit-bucket").waitingFor(Wait.forHttp("/").forPort(9090).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    static final Map<String, Secret> SECRETS = Map.of("AWS_KEYS", Secret.of("kit-access:kit-secret"));
    static S3Client s3;

    private final InMemoryPollCursorStore store = new InMemoryPollCursorStore();
    private final String prefix = "kit/" + UUID.randomUUID() + "/";
    private final Map<String, Integer> files = new ConcurrentSkipListMap<>();
    private int fileNo;

    @BeforeAll
    static void start() {
        S3MOCK.start();
        s3 = S3FileConnector.client(new S3FileConnector.Settings(java.net.URI.create(endpoint()), "us-east-1", "kit-bucket",
                "", true, LineFormat.JSONL, 1, Duration.ofSeconds(1), "t"), config("x"));
    }

    @AfterAll
    static void stop() {
        s3.close();
        S3MOCK.stop();
    }

    static String endpoint() {
        return "http://" + S3MOCK.getHost() + ":" + S3MOCK.getMappedPort(9090);
    }

    static SourceConfig config(String prefix) {
        ObjectNode c = JsonMapper.builder().build().createObjectNode();
        c.put("endpoint", endpoint()).put("bucket", "kit-bucket").put("prefix", prefix).put("format", "jsonl")
                .put("intervalSec", 1).put("batchSize", 100);
        return new SourceConfig(1, 51, SourceTypes.CONNECTOR, S3FileConnector.KEY, c, SECRETS, null);
    }

    @Override
    protected SourceConnector connector() {
        return new S3FileConnector(PollingOptions.defaults().withMinPollInterval(Duration.ofMillis(100)), Clock.systemUTC());
    }

    @Override
    protected SourceConfig sourceConfig() {
        return config(prefix);
    }

    @Override
    protected PollCursorStore cursorStore() {
        return store;
    }

    @Override
    protected ContractPeer peer() {
        return new ContractPeer() {
            @Override
            public void publish(List<byte[]> payloads) {
                StringBuilder sb = new StringBuilder();
                payloads.forEach(p -> sb.append(new String(p, StandardCharsets.UTF_8)).append('\n'));
                String key = prefix + String.format("%06d", fileNo++) + ".jsonl";
                s3.putObject(b -> b.bucket("kit-bucket").key(key), RequestBody.fromString(sb.toString()));
                files.put(key, payloads.size());
            }

            @Override
            public long acknowledgedCount() {
                PollCursor c = store.load(51).orElse(null);
                if (c == null) {
                    return 0;
                }
                long n = files.entrySet().stream().filter(e -> c.cursor() != null && e.getKey().compareTo(c.cursor()) <= 0)
                        .mapToLong(Map.Entry::getValue).sum();
                if (c.pageToken() != null) {
                    n += Long.parseLong(c.pageToken().substring(c.pageToken().lastIndexOf('\n') + 1));
                }
                return n;
            }
        };
    }

    @Test
    @DisplayName("DSC-09.07 CSV 머리글을 키로, 숫자는 숫자로, 따옴표 안 쉼표는 한 칸")
    void csvLines() {
        LineFormat csv = LineFormat.CSV;
        List<String> header = LineFormat.split("deviceId,temp,note");
        assertThat(new String(csv.payload("em300-1,22.5,\"hot, humid\"", header), StandardCharsets.UTF_8))
                .isEqualTo("{\"deviceId\":\"em300-1\",\"temp\":22.5,\"note\":\"hot, humid\"}");
        assertThat(csv.payload("  ", header)).isNull();
    }
}
