package net.java21.data2flow.ingress.source.event;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.ingress.source.service.RuntimeConfigSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;

/**
 * 설정 변경 메시지(EVT-DSC-01·EVT-DEV-04, fanout {@code data2flow.config} → 인스턴스별 임시 큐)를 받는다. 소스·자격증명이 바뀌면
 * core에서 설정을 다시 읽는다(원천은 DB, BR-DSC-05). 다른 종류와 형식이 틀린 메시지는 무시한다(임시 큐라 DLQ가 없다).
 */
public class ConfigChangedListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangedListener.class);

    private final MessageCodec codec;
    private final RuntimeConfigSync sync;
    private final Runnable credentialChanged;

    public ConfigChangedListener(MessageCodec codec, RuntimeConfigSync sync) {
        this(codec, sync, () -> { });
    }

    /** @param credentialChanged 자격(CREDENTIAL)이 바뀌면 부른다: 서명 키 캐시를 바로 다시 읽는다(DSC-03.02, 1분 안 반영) */
    public ConfigChangedListener(MessageCodec codec, RuntimeConfigSync sync, Runnable credentialChanged) {
        this.codec = codec;
        this.sync = sync;
        this.credentialChanged = credentialChanged;
    }

    public void onMessage(Message message) {
        ConfigChangedMessage change;
        try {
            change = codec.read(message.getBody(), ConfigChangedMessage.class);
        } catch (RuntimeException e) {
            log.warn("설정 변경 메시지를 읽을 수 없어 무시합니다: {}", e.getMessage());
            return;
        }
        switch (change.entityType()) {
            case SOURCE, CREDENTIAL -> {
                log.debug("설정 변경 {} {} v{} → 다시 읽기", change.entityType(), change.id(), change.version());
                sync.requestRefresh();
                if (change.entityType() == ConfigChangedMessage.EntityType.CREDENTIAL) {
                    credentialChanged.run();
                }
            }
            default -> {
            }
        }
    }

    /** 소비자가 (다시) 시작됨: 끊긴 동안 놓친 변경을 보완하려고 전체를 다시 읽는다 */
    public void onConsumerStarted() {
        sync.requestRefresh();
        credentialChanged.run();
    }
}
