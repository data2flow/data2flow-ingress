package net.java21.data2flow.ingress.live.service;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.source.service.SourceSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 소스 원본 메시지 실시간 보기(DSC-02.06, API-DSC-52). 스트림 기록이 끝난 메시지를 구독자에게 SSE로 흘린다(디버깅용, 손실 허용).
 *
 * <ul>
 *   <li>구독자마다 초당 {@code maxRatePerSecond}건까지만 보내고, 넘거나 느린 구독자의 메시지는 버린 뒤 {@code dropped{count}}로 알린다.</li>
 *   <li>수신 경로를 막지 않는다: {@link #onReceived}는 큐에 넣기만 하고, 전송은 구독자별 가상 스레드가 한다.</li>
 *   <li>원본 앞 4KB만 보낸다({@code rawExcerpt}).</li>
 * </ul>
 */
public class LiveTap implements SourceSupervisor.ReceivedListener {

    private static final Logger log = LoggerFactory.getLogger(LiveTap.class);
    static final int EXCERPT = 4096;

    private final IngressProperties.Live config;
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    public LiveTap(IngressProperties.Live config) {
        this.config = config;
    }

    /** 실시간 보기 구독. 한도를 넘으면 null */
    public SseEmitter subscribe(long sourceId, String topicFilter) {
        if (subscribers.size() >= config.maxSubscribers()) {
            return null;
        }
        SseEmitter emitter = new SseEmitter(config.timeout().toMillis());
        Subscriber s = new Subscriber(sourceId, topicFilter, emitter);
        subscribers.add(s);
        emitter.onCompletion(() -> s.stop());
        emitter.onTimeout(() -> s.stop());
        emitter.onError(e -> s.stop());
        try {
            emitter.send(SseEmitter.event().comment("subscribed"));   // 응답 헤더를 바로 보낸다
        } catch (IOException e) {
            s.stop();
            return emitter;
        }
        Thread.ofVirtual().name("live-" + sourceId).start(s::run);
        return emitter;
    }

    @Override
    public void onReceived(RawEnvelope envelope) {
        for (Subscriber s : subscribers) {
            if (s.sourceId == envelope.sourceId() && matches(s.topicFilter, envelope.topic())) {
                s.offer(envelope);
            }
        }
    }

    public int subscriberCount() {
        return subscribers.size();
    }

    /** MQTT 토픽 필터({@code +}, {@code #}) 일치 */
    static boolean matches(String filter, String topic) {
        if (filter == null || filter.isBlank() || "#".equals(filter)) {
            return true;
        }
        if (topic == null) {
            return false;
        }
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            if ("#".equals(f[i])) {
                return true;
            }
            if (i >= t.length || (!"+".equals(f[i]) && !f[i].equals(t[i]))) {
                return false;
            }
        }
        return f.length == t.length;
    }

    private final class Subscriber {
        final long sourceId;
        final String topicFilter;
        final SseEmitter emitter;
        final BlockingQueue<RawEnvelope> queue = new ArrayBlockingQueue<>(64);
        final AtomicLong dropped = new AtomicLong();
        volatile boolean active = true;
        long windowStart = System.nanoTime();
        int sentInWindow;

        Subscriber(long sourceId, String topicFilter, SseEmitter emitter) {
            this.sourceId = sourceId;
            this.topicFilter = topicFilter;
            this.emitter = emitter;
        }

        void offer(RawEnvelope e) {
            if (!queue.offer(e)) {
                dropped.incrementAndGet();
            }
        }

        void run() {
            try {
                while (active) {
                    RawEnvelope e = queue.poll(1, TimeUnit.SECONDS);
                    long dropCount = dropped.getAndSet(0);
                    if (dropCount > 0) {
                        emitter.send(SseEmitter.event().name("dropped").data(Map.of("count", dropCount),
                                MediaType.APPLICATION_JSON));
                    }
                    if (e == null) {
                        continue;
                    }
                    long now = System.nanoTime();
                    if (now - windowStart >= 1_000_000_000L) {
                        windowStart = now;
                        sentInWindow = 0;
                    }
                    if (sentInWindow >= config.maxRatePerSecond()) {
                        dropped.incrementAndGet();
                        continue;
                    }
                    sentInWindow++;
                    emitter.send(SseEmitter.event().name("message").data(view(e), MediaType.APPLICATION_JSON));
                }
            } catch (IOException | IllegalStateException e) {
                log.debug("실시간 보기 구독 종료: {}", e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                stop();
            }
        }

        void stop() {
            active = false;
            subscribers.remove(this);
        }
    }

    static Map<String, Object> view(RawEnvelope e) {
        byte[] p = e.payload();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("receivedAt", e.receivedAt().toString());
        m.put("topic", e.topic());
        m.put("size", p.length);
        m.put("rawExcerpt", new String(p, 0, Math.min(p.length, EXCERPT), StandardCharsets.UTF_8));
        return m;
    }
}
