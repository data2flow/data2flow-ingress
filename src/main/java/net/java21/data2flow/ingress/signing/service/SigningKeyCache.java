package net.java21.data2flow.ingress.signing.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 플랫폼 브로커 기기 서명 키 캐시(DSC-03.02·03.03, ADR-042). 시작할 때, {@code refreshInterval}(기본 30초, 60초 이하)마다,
 * 그리고 자격 설정 변경(EVT-DSC-01 CREDENTIAL)을 받으면 1초 안에 core에서 ACTIVE 키 전체를 다시 읽는다. 그래서 폐기한 키는
 * 1분 안에 쓰이지 않는다. 읽기에 실패하면 마지막으로 받은 키를 그대로 쓴다(core 장애로 정상 기기를 거부하지 않도록).
 * <b>키 값은 로그에 남기지 않는다.</b>
 */
public class SigningKeyCache implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SigningKeyCache.class);
    static final Duration DEBOUNCE = Duration.ofSeconds(1);

    /** core에서 키 전체를 읽는다 */
    @FunctionalInterface
    public interface Fetcher {
        List<Key> fetch();
    }

    /** ACTIVE 서명 키 하나. toString은 키 값을 가린다 */
    public record Key(long sourceId, String deviceKey, long deviceId, long credentialId, String signingKey, Instant expiresAt) {
        @Override
        public String toString() {
            return "Key[sourceId=" + sourceId + ", deviceKey=" + deviceKey + ", credentialId=" + credentialId + ", signingKey=***]";
        }
    }

    private record Id(long sourceId, String deviceKey) {
    }

    private final Fetcher fetcher;
    private final Duration refreshInterval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("signing-keys").factory());
    private final AtomicBoolean refreshQueued = new AtomicBoolean();
    private volatile Map<Id, Key> keys = Map.of();
    private volatile Instant loadedAt;

    public SigningKeyCache(Fetcher fetcher, Duration refreshInterval, Clock clock) {
        this.fetcher = fetcher;
        this.refreshInterval = refreshInterval;
        this.clock = clock;
    }

    /** 첫 읽기와 주기 읽기를 시작한다 */
    public void start() {
        long every = refreshInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::refresh, 0, every, TimeUnit.MILLISECONDS);
    }

    /** 자격 설정 변경을 받았다. 짧은 시간 안의 여러 변경은 한 번에 읽는다 */
    public void requestRefresh() {
        if (!refreshQueued.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(() -> {
            refreshQueued.set(false);
            refresh();
        }, DEBOUNCE.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** @return 읽기에 성공했으면 true */
    public boolean refresh() {
        try {
            List<Key> fetched = fetcher.fetch();
            Map<Id, Key> next = new HashMap<>();
            for (Key k : fetched) {
                next.put(new Id(k.sourceId(), k.deviceKey().toLowerCase(Locale.ROOT)), k);
            }
            keys = Map.copyOf(next);
            loadedAt = clock.instant();
            log.debug("서명 키 {}개를 읽었습니다", next.size());
            return true;
        } catch (RuntimeException e) {
            log.warn("서명 키를 읽지 못해 이전 키 {}개를 계속 씁니다: {}", keys.size(), e.getClass().getSimpleName());
            return false;
        }
    }

    /** 이 소스·기기의 ACTIVE 서명 키. 만료된 키는 없는 것으로 본다 */
    public Optional<Key> find(long sourceId, String deviceKey) {
        if (deviceKey == null) {
            return Optional.empty();
        }
        Key k = keys.get(new Id(sourceId, deviceKey.toLowerCase(Locale.ROOT)));
        if (k == null || k.expiresAt() != null && !k.expiresAt().isAfter(clock.instant())) {
            return Optional.empty();
        }
        return Optional.of(k);
    }

    public int size() {
        return keys.size();
    }

    /** 마지막으로 읽기에 성공한 시각. 없으면 null */
    public Instant loadedAt() {
        return loadedAt;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
