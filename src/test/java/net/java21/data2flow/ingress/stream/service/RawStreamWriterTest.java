package net.java21.data2flow.ingress.stream.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.ingress.common.IngressProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RawStreamWriterTest {

    @Test
    @DisplayName("ING-01.03 RabbitMQ에 닿지 않으면 준비되지 않음(readiness DOWN)이고, 기록은 바로 실패한다 → 커넥터는 확인하지 않는다")
    void unavailableStreamFailsFast() throws Exception {
        int closed;
        try (ServerSocket s = new ServerSocket(0)) {
            closed = s.getLocalPort();
        }
        RawStreamWriter writer = new RawStreamWriter(
                new IngressProperties.Stream("127.0.0.1", closed, "/", "guest", "guest", true, true, 10),
                Duration.ofSeconds(1), SuperStreamSpec.RAW, MessageCodec.create(), MessageTracing.noop(),
                new SimpleMeterRegistry());
        writer.start();
        assertThat(writer.isRunning()).isTrue();
        assertThat(writer.getPhase()).isEqualTo(RawStreamWriter.PHASE);
        assertThat(writer.isReady()).isFalse();
        RawEnvelope e = RawEnvelope.of(1, 1, "MQTT_SUBSCRIBE", "t", new byte[]{1}, Instant.EPOCH, "i", "k");
        assertThatThrownBy(() -> writer.write(e).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RawStreamWriter.StreamUnavailableException.class);
        assertThat(writer.unconfirmed()).isZero();
        writer.stop();
        assertThat(writer.isRunning()).isFalse();
    }
}
