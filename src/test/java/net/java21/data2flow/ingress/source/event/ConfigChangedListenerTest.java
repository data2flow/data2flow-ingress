package net.java21.data2flow.ingress.source.event;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.ingress.source.service.RuntimeConfigSync;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigChangedListenerTest {

    static final MessageCodec CODEC = MessageCodec.create();
    final AtomicInteger refreshes = new AtomicInteger();
    final RuntimeConfigSync sync = new RuntimeConfigSync(null, null, null, null) {
        @Override
        public void requestRefresh() {
            refreshes.incrementAndGet();
        }
    };
    final ConfigChangedListener listener = new ConfigChangedListener(CODEC, sync);

    static Message message(byte[] body) {
        return new Message(body, new MessageProperties());
    }

    @Test
    @DisplayName("BR-DSC-05 EVT-DSC-01 SOURCE·CREDENTIAL 변경이면 설정을 다시 읽고, 다른 종류·형식 오류·모르는 종류는 무시한다")
    void reactsToSourceChanges() {
        listener.onMessage(message(CODEC.write(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SOURCE, 3, 2, 1, Clock.systemUTC()))));
        listener.onMessage(message(CODEC.write(ConfigChangedMessage.delete(ConfigChangedMessage.EntityType.CREDENTIAL, 9, 1, 1, Clock.systemUTC()))));
        listener.onMessage(message(CODEC.write(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.DEVICE, 5, 1, 1, Clock.systemUTC()))));
        listener.onMessage(message("{broken".getBytes(StandardCharsets.UTF_8)));
        listener.onMessage(message(("{\"v\":1,\"messageId\":\"8f3e0000-0000-4000-8000-000000000000\",\"entityType\":\"NEW_THING\","
                + "\"id\":\"1\",\"version\":1,\"op\":\"UPSERT\",\"orgId\":\"1\",\"at\":\"2026-10-04T00:00:00Z\"}")
                .getBytes(StandardCharsets.UTF_8)));
        assertThat(refreshes).hasValue(2);
        listener.onConsumerStarted();
        assertThat(refreshes).hasValue(3);
    }
}
