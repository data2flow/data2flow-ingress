package net.java21.data2flow.ingress.payload.codec;

import tools.jackson.databind.JsonNode;

/**
 * 받은 바이트(압축은 이미 푼 것)를 구조화된 JSON 값으로 바꾼다(connectors.md §1 PayloadCodec, DSC-09.07). 구현은 스레드 안전해야 한다
 * (한 소스의 메시지가 여러 스레드에서 올 수 있다).
 */
public interface PayloadCodec {

    /**
     * @param payload 압축을 푼 바이트
     * @param topic   토픽·경로(없으면 null). Sparkplug B처럼 토픽에 따라 해석이 달라지는 형식이 쓴다
     */
    JsonNode decode(byte[] payload, String topic) throws PayloadDecodeException;
}
