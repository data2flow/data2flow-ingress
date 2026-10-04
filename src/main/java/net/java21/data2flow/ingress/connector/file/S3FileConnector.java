package net.java21.data2flow.ingress.connector.file;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorSession;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.common.Cfg;
import net.java21.data2flow.ingress.connector.common.ConnectorSchemas;
import net.java21.data2flow.ingress.connector.common.LoopSession;
import net.java21.data2flow.ingress.connector.common.PollingOptions;
import net.java21.data2flow.ingress.connector.common.StagedConnectionTest;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import tools.jackson.databind.JsonNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 파일 가져오기 커넥터(키 {@code file-s3}, DSC-09 "파일·일괄", connectors.md §2 AWS SDK v2 Apache-2.0). S3 호환 저장소(AWS S3, MinIO,
 * Ceph 등)의 접두사 아래 파일을 키 순서대로 읽어 한 줄을 원본 하나로 넘긴다(형식 {@code jsonl}·{@code csv}).
 *
 * <p>무손실(CURSOR): 줄들을 기록(confirm)한 뒤에만 위치를 저장한다. 위치 = 다 읽은 마지막 파일 키({@code cursor})와 읽는 중인 파일의
 * {@code 키\n줄 번호}({@code pageToken}). 재시작하면 그 줄 다음부터 읽고, 파일은 지우거나 옮기지 않는다(읽기 전용). 리더 1대만 실행한다.
 * SFTP·Parquet은 아직 없다.
 */
public class S3FileConnector implements SourceConnector {

    public static final String KEY = "file-s3";
    private static final JsonNode SCHEMA = ConnectorSchemas.load(KEY);

    private final PollingOptions options;
    private final Clock clock;

    public S3FileConnector(PollingOptions options, Clock clock) {
        this.options = options;
        this.clock = clock;
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor(KEY, "파일 가져오기(S3 호환)", "1.0.0", ConnectorCategory.FILE,
                Set.of(AuthMethod.AWS_SIGV4), Set.of(PayloadFormat.JSON, PayloadFormat.CSV), AckMode.CURSOR,
                ScalingMode.SINGLETON, false);
    }

    @Override
    public JsonNode configSchema() {
        return SCHEMA;
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        Settings s = Settings.from(config, options);
        URI endpoint = s.endpoint != null ? s.endpoint : URI.create("https://s3." + s.region + ".amazonaws.com");
        int port = endpoint.getPort() > 0 ? endpoint.getPort() : "https".equals(endpoint.getScheme()) ? 443 : 80;
        return new StagedConnectionTest<Client>(endpoint.getHost(), port, false, null, true, options.testTimeout(), "POLL")
                .run(remaining -> {
                    Client c = new Client(client(s, config));
                    c.s3.headBucket(b -> b.bucket(s.bucket));
                    return c;
                }, (c, remaining) -> {
                    List<ConnectionTestResult.Preview> preview = new ArrayList<>();
                    for (S3Object o : c.s3.listObjectsV2(ListObjectsV2Request.builder().bucket(s.bucket).prefix(s.prefix)
                            .maxKeys(ConnectionTestResult.MAX_PREVIEW).build()).contents()) {
                        preview.add(StagedConnectionTest.preview(clock, o.key(),
                                (o.key() + " (" + o.size() + " bytes)").getBytes(StandardCharsets.UTF_8)));
                    }
                    return preview;
                }, false);
    }

    private record Client(S3Client s3) implements AutoCloseable {
        @Override
        public void close() {
            s3.close();
        }
    }

    @Override
    public ConnectorSession open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        return new Session(config, Settings.from(config, options), sink, ctx);
    }

    static S3Client client(Settings s, SourceConfig config) {
        Cfg c = Cfg.of(config);
        String id = c.reveal("ACCESS_KEY_ID");
        String secret = c.reveal("SECRET_ACCESS_KEY");
        String pair = c.reveal("AWS_KEYS");
        if ((id == null || secret == null) && pair != null && pair.contains(":")) {
            id = pair.substring(0, pair.indexOf(':'));
            secret = pair.substring(pair.indexOf(':') + 1);
        }
        if (id == null || secret == null) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.SECRET_REQUIRED, "AWS_KEYS");
        }
        var b = S3Client.builder().region(Region.of(s.region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(id, secret)))
                .httpClient(UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofSeconds(60)).build())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .forcePathStyle(s.pathStyle);
        if (s.endpoint != null) {
            b.endpointOverride(s.endpoint);
        }
        return b.build();
    }

    record Settings(URI endpoint, String region, String bucket, String prefix, boolean pathStyle, LineFormat format,
                    int batch, Duration interval, String topic) {
        static Settings from(SourceConfig config, PollingOptions options) {
            Cfg c = Cfg.of(config);
            String ep = c.text("endpoint", null);
            URI endpoint = ep == null ? null : URI.create(ep);
            if (endpoint != null && (endpoint.getHost() == null || !Set.of("http", "https").contains(endpoint.getScheme()))) {
                throw InvalidSettingsException.config("endpoint");
            }
            String bucket = c.required("bucket");
            if (!bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")) {
                throw InvalidSettingsException.config("bucket");
            }
            return new Settings(endpoint, c.text("region", "us-east-1"), bucket, c.text("prefix", ""),
                    c.bool("pathStyle", endpoint != null), LineFormat.of(c.text("format", "jsonl")),
                    c.integer("batchSize", 500, 1, 10_000), options.pollInterval(c.seconds("intervalSec", 300, 1, 86_400)),
                    c.text("topic", "file/" + bucket));
        }
    }

    private final class Session extends LoopSession {
        private final Settings settings;
        private S3Client s3;

        Session(SourceConfig config, Settings settings, RawSink sink, ConnectorContext ctx) {
            super(config, sink, ctx, standardBackoff(), settings.interval, options.writeRetryDelay());
            this.settings = settings;
        }

        @Override
        protected void connect() {
            s3 = client(settings, config);
            s3.headBucket(b -> b.bucket(settings.bucket));
        }

        @Override
        protected boolean pollOnce() throws Exception {
            PollCursor at = ctx.cursorStore().load(config.sourceId()).orElse(PollCursor.initial());
            String fileKey;
            long skip;
            if (at.pageToken() != null) {
                int nl = at.pageToken().lastIndexOf('\n');
                fileKey = at.pageToken().substring(0, nl);
                skip = Long.parseLong(at.pageToken().substring(nl + 1));
            } else {
                ListObjectsV2Request.Builder list = ListObjectsV2Request.builder().bucket(settings.bucket)
                        .prefix(settings.prefix).maxKeys(1);
                if (at.cursor() != null) {
                    list.startAfter(at.cursor());
                }
                List<S3Object> next = s3.listObjectsV2(list.build()).contents();
                if (next.isEmpty()) {
                    return false;
                }
                fileKey = next.get(0).key();
                skip = 0;
            }
            if (isPaused()) {
                return false;
            }
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    s3.getObject(GetObjectRequest.builder().bucket(settings.bucket).key(fileKey).build()),
                    StandardCharsets.UTF_8))) {
                List<String> header = List.of();
                long lineNo = 0;
                String line;
                if (settings.format.hasHeader()) {
                    String h = r.readLine();
                    lineNo++;
                    header = h == null ? List.of() : LineFormat.split(h.replace("﻿", ""));
                }
                while (lineNo < skip && r.readLine() != null) {
                    lineNo++;
                }
                lineNo = Math.max(lineNo, skip);
                List<RawEnvelope> batch = new ArrayList<>();
                while (batch.size() < settings.batch && (line = r.readLine()) != null) {
                    lineNo++;
                    byte[] payload = settings.format.payload(line, header);
                    if (payload != null) {
                        batch.add(envelope(fileKey, payload));
                    }
                }
                boolean finished = r.readLine() == null;
                writeAll(batch);   // 실패하면 위치를 저장하지 않는다(BR-DSC-24)
                PollCursor next = finished ? PollCursor.at(fileKey) : new PollCursor(at.cursor(), fileKey + "\n" + lineNo);
                ctx.cursorStore().save(config.sourceId(), next);
                return true;
            }
        }

        @Override
        protected void disconnect() {
            if (s3 != null) {
                s3.close();
                s3 = null;
            }
        }
    }
}
