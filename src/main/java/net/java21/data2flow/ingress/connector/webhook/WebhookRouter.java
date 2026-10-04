package net.java21.data2flow.ingress.connector.webhook;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 경로의 {@code {sourceKey}} → 실행 중인 Webhook 세션(DSC-01.03). 없는 키는 404로 답한다(어떤 소스가 있는지 드러내지 않음) */
public class WebhookRouter {

    private final Map<String, WebhookConnector.Session> sessions = new ConcurrentHashMap<>();

    void register(String sourceKey, WebhookConnector.Session session) {
        sessions.put(sourceKey, session);
    }

    void unregister(String sourceKey, WebhookConnector.Session session) {
        sessions.remove(sourceKey, session);
    }

    public Optional<WebhookConnector.Session> find(String sourceKey) {
        return Optional.ofNullable(sourceKey == null ? null : sessions.get(sourceKey));
    }
}
