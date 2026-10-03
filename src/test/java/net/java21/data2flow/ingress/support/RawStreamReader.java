package net.java21.data2flow.ingress.support;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 테스트용: {@code data2flow.raw} 12개 파티션을 처음부터 읽어 RawEnvelope와 헤더·파티션을 모은다(pipeline 흉내, 오프셋 저장 없음).
 */
public final class RawStreamReader implements AutoCloseable {

    public record Received(RawEnvelope envelope, String partition, Map<String, Object> headers) {
    }

    private static final MessageCodec CODEC = MessageCodec.create();
    private final List<Consumer> consumers = new ArrayList<>();
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> perPartition = new ConcurrentHashMap<>();

    public RawStreamReader(Environment env) {
        SuperStreamSpec spec = SuperStreamSpec.RAW;
        for (int i = 0; i < spec.partitions(); i++) {
            String partition = spec.partition(i);
            consumers.add(env.consumerBuilder().stream(partition).offset(OffsetSpecification.first())
                    .messageHandler((ctx, message) -> {
                        RawEnvelope e = CODEC.read(message.getBodyAsBinary(), RawEnvelope.class);
                        received.add(new Received(e, partition, message.getApplicationProperties()));
                        perPartition.merge(partition, 1, Integer::sum);
                    }).build());
        }
    }

    public List<Received> received() {
        return List.copyOf(received);
    }

    public List<RawEnvelope> envelopes() {
        return received.stream().map(Received::envelope).toList();
    }

    @Override
    public void close() {
        consumers.forEach(c -> {
            try {
                c.close();
            } catch (RuntimeException ignored) {
                // 무시
            }
        });
    }
}
