package net.java21.data2flow.ingress.stream.service;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.tracing.Span;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.domain.Backoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 원본 기록 창구 {@link RawSink}의 ingress 구현(ING-01.01, EVT-ING-01, DSC-09.03). {@code data2flow.raw} Super Stream에 발행하고
 * <b>publisher confirm을 받으면</b> 완료한다. 커넥터는 완료된 뒤에만 상대에게 확인(PUBACK)을 보낸다.
 *
 * <ul>
 *   <li>라우팅 키: {@link RawEnvelope#routingKey()}({@code sha1(sourceId + topic)}) → 해시로 12개 파티션 중 하나. 같은 기기는 같은 파티션.</li>
 *   <li>헤더: {@link MessageHeaders#of}(messageId·v·schema·organizationId) + W3C {@code traceparent}(OPS-02.03).</li>
 *   <li>confirm 제한 시간(10초)을 넘거나 브로커가 거부하면 실패로 완료한다. 생산자 내부 재전송은 끈다(retryOnRecovery=false):
 *       실패하면 MQTT 쪽이 확인하지 않고 다시 받아 새로 기록한다.</li>
 *   <li>역압: confirm을 기다리는 메시지가 {@code maxUnconfirmed}를 넘으면 발행이 기다린다.</li>
 *   <li>RabbitMQ가 없으면 서비스는 뜨되 readiness가 DOWN이고, 백오프로 다시 연결한다.</li>
 * </ul>
 *
 * <p>생산자 이름(publishing ID 중복 제거)은 쓰지 않는다. 이 구현은 같은 메시지를 생산자 수준에서 다시 보내지 않으므로 거를 중복이 없고,
 * 같은 이름을 두 인스턴스가 쓰면 정상 메시지가 중복으로 버려질 위험만 생긴다. 이중 수신 중복은 dedupKey로 pipeline이 거른다(reliability-and-ha.md §2.1).
 */
public class RawStreamWriter implements RawSink, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RawStreamWriter.class);
    /** 파티션 선택용 애플리케이션 속성(라우팅 키). 소비자는 무시한다 */
    public static final String ROUTING_KEY_HEADER = "routingKey";
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 200;

    private final IngressProperties.Stream config;
    private final Duration confirmTimeout;
    private final SuperStreamSpec spec;
    private final MessageCodec codec;
    private final MessageTracing tracing;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("raw-stream-init").factory());
    private final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(30), Integer.MAX_VALUE,
            Duration.ofSeconds(30));
    private final AtomicInteger unconfirmed = new AtomicInteger();
    private final Counter confirmed;
    private final Counter failed;
    private final Timer confirmLatency;

    private volatile Environment environment;
    private volatile Producer producer;
    private volatile boolean running;
    private volatile String lastError;

    public RawStreamWriter(IngressProperties.Stream config, Duration confirmTimeout, SuperStreamSpec spec,
                           MessageCodec codec, MessageTracing tracing, MeterRegistry registry) {
        this.config = config;
        this.confirmTimeout = confirmTimeout;
        this.spec = spec;
        this.codec = codec;
        this.tracing = tracing;
        this.confirmed = Counter.builder("data2flow.ingress.raw.published").tag("result", "confirmed")
                .description("data2flow.raw 기록 결과(EVT-ING-01)").register(registry);
        this.failed = Counter.builder("data2flow.ingress.raw.published").tag("result", "failed")
                .description("data2flow.raw 기록 결과(EVT-ING-01)").register(registry);
        this.confirmLatency = Timer.builder("data2flow.ingress.raw.confirm")
                .description("발행에서 publisher confirm까지 걸린 시간").publishPercentiles(0.5, 0.95).register(registry);
        Gauge.builder("data2flow.ingress.raw.unconfirmed", unconfirmed, AtomicInteger::get)
                .description("confirm을 기다리는 메시지 수").register(registry);
    }

    @Override
    public CompletionStage<Void> write(RawEnvelope envelope) {
        Producer p = producer;
        if (p == null) {
            failed.increment();
            return CompletableFuture.failedFuture(new StreamUnavailableException("data2flow.raw 생산자가 준비되지 않았습니다"
                    + (lastError == null ? "" : ": " + lastError)));
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        Map<String, Object> headers = MessageHeaders.of(envelope);
        Span span = tracing.startProducerSpan(spec.name(), headers);
        long started = System.nanoTime();
        try {
            MessageBuilder mb = p.messageBuilder();
            mb.properties().messageId(envelope.messageId().toString()).contentType("application/json");
            MessageBuilder.ApplicationPropertiesBuilder props = mb.applicationProperties();
            headers.forEach((k, v) -> props.entry(k, String.valueOf(v)));
            props.entry(ROUTING_KEY_HEADER, envelope.routingKey());
            Message message = props.messageBuilder().addData(codec.write(envelope)).build();
            unconfirmed.incrementAndGet();
            p.send(message, status -> {
                unconfirmed.decrementAndGet();
                confirmLatency.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
                if (status.isConfirmed()) {
                    confirmed.increment();
                    tracing.end(span, null);
                    result.complete(null);
                } else {
                    failed.increment();
                    StreamWriteException e = new StreamWriteException("data2flow.raw confirm 실패(code "
                            + status.getCode() + ")");
                    tracing.end(span, e);
                    result.completeExceptionally(e);
                }
            });
        } catch (RuntimeException e) {
            failed.increment();
            tracing.end(span, e);
            result.completeExceptionally(e);
        }
        return result;
    }

    /** 발행할 수 있는 상태인가(readiness) */
    public boolean isReady() {
        return producer != null;
    }

    public String lastError() {
        return lastError;
    }

    public int unconfirmed() {
        return unconfirmed.get();
    }

    @Override
    public void start() {
        running = true;
        scheduler.execute(this::initialize);
    }

    private void initialize() {
        if (!running || producer != null) {
            return;
        }
        try {
            EnvironmentBuilder b = Environment.builder().host(config.host()).port(config.port())
                    .virtualHost(config.virtualHost()).username(config.username()).password(config.password());
            if (config.useConfiguredAddress()) {
                Address fixed = new Address(config.host(), config.port());
                b.addressResolver(address -> fixed);
            }
            Environment env = b.build();
            if (config.createSuperStream()) {
                createSuperStreamIfMissing(env);
            }
            Producer p = env.producerBuilder().superStream(spec.name())
                    .routing(m -> String.valueOf(m.getApplicationProperties().get(ROUTING_KEY_HEADER))).producerBuilder()
                    .confirmTimeout(confirmTimeout).maxUnconfirmedMessages(config.maxUnconfirmed())
                    .retryOnRecovery(false).build();
            environment = env;
            producer = p;
            lastError = null;
            backoff.reset();
            log.info("data2flow.raw 생산자 준비됨({}:{} vhost {})", config.host(), config.port(), config.virtualHost());
        } catch (RuntimeException e) {
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            Duration delay = backoff.nextDelay(false);
            log.warn("data2flow.raw 생산자를 만들지 못했습니다({}), {}ms 뒤 다시 시도", lastError, delay.toMillis());
            closeQuietly();
            if (running) {
                scheduler.schedule(this::initialize, delay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void createSuperStreamIfMissing(Environment env) {
        try {
            env.streamCreator().name(spec.name()).superStream().partitions(spec.partitions()).creator()
                    .maxAge(spec.maxAge()).maxLengthBytes(ByteCapacity.B(spec.maxBytesPerPartition())).create();
            log.info("Super Stream {}를 만들었습니다({} 파티션)", spec.name(), spec.partitions());
        } catch (StreamException e) {
            if (e.getCode() != Constants.RESPONSE_CODE_STREAM_ALREADY_EXISTS) {
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        closeQuietly();
        scheduler.shutdownNow();
    }

    private void closeQuietly() {
        Producer p = producer;
        producer = null;
        Environment env = environment;
        environment = null;
        try {
            if (p != null) {
                p.close();
            }
        } catch (RuntimeException e) {
            log.debug("생산자 닫기 실패: {}", e.toString());
        }
        try {
            if (env != null) {
                env.close();
            }
        } catch (RuntimeException e) {
            log.debug("스트림 환경 닫기 실패: {}", e.toString());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /** 생산자가 아직 없다 */
    public static final class StreamUnavailableException extends RuntimeException {
        public StreamUnavailableException(String message) {
            super(message);
        }
    }

    /** confirm이 거부·시간 초과로 끝났다 */
    public static final class StreamWriteException extends RuntimeException {
        public StreamWriteException(String message) {
            super(message);
        }
    }
}
