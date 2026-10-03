package net.java21.data2flow.ingress.connector.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class AckSequencerTest {

    private final AckSequencer acks = new AckSequencer();
    private final List<Integer> sent = new CopyOnWriteArrayList<>();

    private long register(int n) {
        return acks.register(() -> sent.add(n));
    }

    @Test
    @DisplayName("DSC-09.03 BR-DSC-24 TC-DSC-252 기록이 끝나기 전에는 확인하지 않고, 끝나면 받은 순서대로 확인한다")
    void acknowledgesInArrivalOrderAfterWrite() {
        long a = register(1);
        long b = register(2);
        long c = register(3);
        assertThat(sent).isEmpty();
        acks.complete(c);
        acks.complete(b);
        assertThat(sent).as("앞선 메시지(1)가 기록 중이면 뒤의 것도 확인하지 않는다").isEmpty();
        assertThat(acks.inFlight()).isEqualTo(1);
        acks.complete(a);
        assertThat(sent).containsExactly(1, 2, 3);
        assertThat(acks.outstanding()).isZero();
    }

    @Test
    @DisplayName("DSC-09.03 TC-ING-016 기록에 실패하면 그 뒤로는 이 연결에서 확인하지 않는다(재접속해 재전송을 받는다)")
    void failureStopsAcknowledgements() {
        long a = register(1);
        long b = register(2);
        long c = register(3);
        acks.complete(a);
        acks.fail(b);
        acks.complete(c);
        assertThat(sent).containsExactly(1);
        assertThat(acks.failed()).isTrue();
    }

    @Test
    @DisplayName("TC-ING-021 drain은 기록 중인 메시지가 모두 끝나고 확인까지 보낸 뒤 돌아온다")
    void drainWaitsForInFlight() throws Exception {
        long a = register(1);
        Thread t = Thread.ofVirtual().start(() -> acks.complete(a));
        assertThat(acks.drain(5_000)).isTrue();
        t.join();
        assertThat(sent).containsExactly(1);
    }

    @Test
    @DisplayName("TC-ING-021 drain은 제한 시간이 지나면 false")
    void drainTimesOut() throws Exception {
        register(1);
        assertThat(acks.drain(50)).isFalse();
    }

    @Test
    @DisplayName("모르는 순번 완료는 무시한다")
    void unknownSequenceIgnored() {
        acks.complete(99);
        assertThat(sent).isEmpty();
    }
}
