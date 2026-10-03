package net.java21.data2flow.ingress.source.service;

import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.connector.domain.Backoff;
import net.java21.data2flow.ingress.source.dto.RuntimeConfigSnapshot;
import net.java21.data2flow.ingress.stream.service.RawStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * core-api 설정(API-DSC-50)을 읽어 {@link SourceSupervisor}에 반영한다(BR-DSC-05: 활성 소스는 1분 이내 연결 시도).
 *
 * <ul>
 *   <li>시작: 스트림 생산자가 준비되면(기록할 곳이 있어야 확인할 수 있다) 전체 설정을 읽는다. 실패하면 1, 2, 4 … 60초 간격으로 다시 시도.</li>
 *   <li>설정 변경 메시지(EVT-DSC-01, {@code data2flow.config} entityType SOURCE·CREDENTIAL)를 받으면 1초 안에 모아 한 번 읽는다.</li>
 *   <li>메시지를 놓쳐도 {@code resyncInterval}(5분)마다 다시 읽어 맞춘다(원천은 DB).</li>
 * </ul>
 */
public class RuntimeConfigSync implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RuntimeConfigSync.class);
    static final Duration DEBOUNCE = Duration.ofSeconds(1);

    private final CoreSourceClient core;
    private final SourceSupervisor supervisor;
    private final RawStreamWriter writer;
    private final IngressProperties properties;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("runtime-config").factory());
    private final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), Integer.MAX_VALUE,
            Duration.ofSeconds(60));
    private final AtomicBoolean refreshQueued = new AtomicBoolean();
    private volatile String version;
    private volatile boolean loaded;
    private ScheduledFuture<?> periodic;

    public RuntimeConfigSync(CoreSourceClient core, SourceSupervisor supervisor, RawStreamWriter writer,
                             IngressProperties properties) {
        this.core = core;
        this.supervisor = supervisor;
        this.writer = writer;
        this.properties = properties;
    }

    /** 시작(ApplicationReady). 스트림이 준비될 때까지 기다린 뒤 첫 설정을 읽는다 */
    public void start() {
        scheduler.execute(this::initialLoad);
    }

    private void initialLoad() {
        if (!writer.isReady()) {
            scheduler.schedule(this::initialLoad, 500, TimeUnit.MILLISECONDS);
            return;
        }
        if (refresh(true)) {
            long every = properties.resyncInterval().toMillis();
            periodic = scheduler.scheduleWithFixedDelay(() -> refresh(false), every, every, TimeUnit.MILLISECONDS);
        } else {
            Duration delay = backoff.nextDelay(false);
            log.warn("core 설정을 읽지 못해 {}ms 뒤 다시 시도합니다", delay.toMillis());
            scheduler.schedule(this::initialLoad, delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** 설정 변경 메시지를 받았다. 짧은 시간 안의 여러 변경은 한 번에 읽는다 */
    public void requestRefresh() {
        if (!loaded || !refreshQueued.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(() -> {
            refreshQueued.set(false);
            if (!refresh(false)) {
                scheduler.schedule(this::requestRefresh, backoff.nextDelay(false).toMillis(), TimeUnit.MILLISECONDS);
            }
        }, DEBOUNCE.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @param full true면 sinceVersion 없이 전체를 읽는다
     * @return 읽기에 성공했으면 true
     */
    boolean refresh(boolean full) {
        try {
            Optional<RuntimeConfigSnapshot> snapshot = core.fetch(full ? null : version);
            snapshot.ifPresent(s -> {
                supervisor.apply(s);
                version = s.version();
                log.info("소스 실행 설정 반영(버전 {}, 소스 {}개)", s.version(), s.sources().size());
            });
            loaded = true;
            backoff.reset();
            return true;
        } catch (RuntimeException e) {
            log.warn("core 소스 실행 설정 조회 실패: {}", e.toString());
            return false;
        }
    }

    public boolean loaded() {
        return loaded;
    }

    @Override
    public void close() {
        if (periodic != null) {
            periodic.cancel(false);
        }
        scheduler.shutdownNow();
    }
}
