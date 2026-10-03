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
import org.springframework.amqp.rabbit.core.RabbitTemplate;

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
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setMessageId(event.messageId().toString());
        MessageHeaders.of(event).forEach(props::setHeader);
        try {
            rabbit.send(MessagingNames.EXCHANGE_EVENTS, event.type(), new Message(codec.write(event), props));
            published.increment();
        } catch (AmqpException e) {
            failed.increment();
            log.debug("이벤트 {} 발행 실패: {}", event.type(), e.toString());
        }
    }
}
