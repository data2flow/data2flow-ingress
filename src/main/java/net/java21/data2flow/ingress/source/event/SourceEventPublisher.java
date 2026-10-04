package net.java21.data2flow.ingress.source.event;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * 도메인 이벤트를 {@code data2flow.events}(topic)에 발행한다. 라우팅 키는 이벤트 {@code type}, 헤더는 {@link MessageHeaders#of}.
 * 상태 보고(EVT-DSC-02·03·09)는 주기적으로 다시 보내므로 발행 실패는 기록만 하고 넘어간다(RabbitMQ가 없어도 수집은 계속).
 */
public class SourceEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SourceEventPublisher.class);

    private final RabbitTemplate rabbit;
    private final MessageCodec codec;
    private final Counter published;
    private final Counter failed;

    public SourceEventPublisher(RabbitTemplate rabbit, MessageCodec codec, MeterRegistry registry) {
        this.rabbit = rabbit;
        this.codec = codec;
        this.published = Counter.builder("data2flow.ingress.events.published").tag("result", "ok").register(registry);
        this.failed = Counter.builder("data2flow.ingress.events.published").tag("result", "failed").register(registry);
    }

    public void publish(DomainEvent<?> event) {
        try {
            rabbit.send(MessagingNames.EXCHANGE_EVENTS, event.type(), message(event));
            published.increment();
        } catch (AmqpException e) {
            failed.increment();
            log.debug("이벤트 {} 발행 실패: {}", event.type(), e.toString());
        }
    }

    /**
     * 브로커 확인(publisher confirm)을 받으면 끝나는 발행. 유실되면 안 되는 이벤트(EVT-ACT-09 다운링크 결과)에 쓴다: 호출 쪽은 이 결과가
     * 정상으로 끝난 뒤에만 원본(MQTT)을 확인한다. nack·연결 오류는 실패로 끝나고, 시간 제한은 호출 쪽이 건다.
     * {@code spring.rabbitmq.publisher-confirm-type=correlated}가 필요하다.
     */
    public CompletableFuture<Void> publishConfirmed(DomainEvent<?> event) {
        CorrelationData correlation = new CorrelationData(event.messageId().toString());
        try {
            rabbit.send(MessagingNames.EXCHANGE_EVENTS, event.type(), message(event), correlation);
        } catch (AmqpException e) {
            failed.increment();
            return CompletableFuture.failedFuture(e);
        }
        return correlation.getFuture().thenApply(confirm -> {
            if (!confirm.ack()) {
                failed.increment();
                throw new AmqpException("브로커가 이벤트 " + event.type() + "를 거부했습니다(nack): " + confirm.reason());
            }
            published.increment();
            return null;
        });
    }

    private Message message(DomainEvent<?> event) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setMessageId(event.messageId().toString());
        MessageHeaders.of(event).forEach(props::setHeader);
        return new Message(codec.write(event), props);
    }
}
