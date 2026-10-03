package net.java21.data2flow.ingress.connector.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * 받은 순서대로 확인(PUBACK)을 보내는 장치(reliability-and-ha.md ②, MQTT 3.1.1 §4.6·MQTT 5 §4.6 "받은 순서대로 PUBACK").
 *
 * <p>메시지를 받으면 {@link #register}로 순번을 받고, 스트림 기록(confirm)이 끝나면 {@link #complete}를 부른다.
 * 앞선 메시지가 모두 끝난 구간만 확인을 보낸다. 하나라도 기록에 실패하면({@link #fail}) 그 뒤로는 이 연결에서 확인을 보내지 않는다.
 * 그 메시지를 확인하지 않은 채 뒤의 것을 확인할 수 없기 때문이다. 호출하는 쪽은 연결을 끊고 영속 세션으로 다시 접속해 재전송을 받는다.
 *
 * <p>확인 동작은 잠금 밖에서 순서대로 실행한다. 스레드 안전.
 */
public final class AckSequencer {

    private final TreeMap<Long, Entry> pending = new TreeMap<>();
    private long nextSeq;
    private boolean failed;
    private final Object ackOrder = new Object();

    private static final class Entry {
        final Runnable ack;
        boolean done;

        Entry(Runnable ack) {
            this.ack = ack;
        }
    }

    /** 받은 메시지를 등록한다. 받은 순서대로 불러야 한다 */
    public synchronized long register(Runnable ack) {
        long seq = nextSeq++;
        pending.put(seq, new Entry(ack));
        return seq;
    }

    /** 기록이 끝났다. 앞에서부터 끝난 것들의 확인을 보낸다 */
    public void complete(long seq) {
        synchronized (ackOrder) {
            List<Runnable> toAck = new ArrayList<>();
            synchronized (this) {
                Entry e = pending.get(seq);
                if (e == null) {
                    return;
                }
                e.done = true;
                notifyAll();
                if (failed) {
                    return;
                }
                while (!pending.isEmpty() && pending.firstEntry().getValue().done) {
                    toAck.add(pending.pollFirstEntry().getValue().ack);
                }
                notifyAll();
            }
            toAck.forEach(Runnable::run);
        }
    }

    /** 기록에 실패했다. 이 연결에서는 더 이상 확인을 보내지 않는다 */
    public synchronized void fail(long seq) {
        failed = true;
        Entry e = pending.get(seq);
        if (e != null) {
            e.done = true;
        }
        notifyAll();
    }

    public synchronized boolean failed() {
        return failed;
    }

    /** 확인을 기다리는(기록 중이거나 앞선 것 때문에 막힌) 메시지 수 */
    public synchronized int outstanding() {
        return pending.size();
    }

    /** 기록 중인 메시지(아직 confirm도 실패도 없는 것) 수 */
    public synchronized int inFlight() {
        return (int) pending.values().stream().filter(e -> !e.done).count();
    }

    /**
     * 기록 중인 메시지가 모두 끝나고 그 확인까지 보낼 때까지 최대 {@code timeoutMillis} 기다린다(graceful shutdown, TC-ING-021).
     *
     * @return 모두 끝났으면 true
     */
    public boolean drain(long timeoutMillis) throws InterruptedException {
        boolean idle = awaitIdle(timeoutMillis);
        synchronized (ackOrder) {
            return idle;   // 진행 중인 확인 전송이 끝날 때까지 기다린다
        }
    }

    /**
     * 기록 중인 메시지가 모두 끝날 때까지 최대 {@code timeoutMillis} 기다린다(graceful shutdown, TC-ING-021).
     *
     * @return 모두 끝났으면 true
     */
    public synchronized boolean awaitIdle(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (inFlight() > 0) {
            long left = (deadline - System.nanoTime()) / 1_000_000L;
            if (left <= 0) {
                return false;
            }
            wait(left);
        }
        return true;
    }
}
