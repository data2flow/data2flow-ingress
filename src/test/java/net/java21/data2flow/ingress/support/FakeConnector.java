package net.java21.data2flow.ingress.support;

import net.java21.data2flow.contracts.connector.AckMode;
import net.java21.data2flow.contracts.connector.AuthMethod;
import net.java21.data2flow.contracts.connector.ConnectionTestResult;
import net.java21.data2flow.contracts.connector.ConnectorCategory;
import net.java21.data2flow.contracts.connector.ConnectorContext;
import net.java21.data2flow.contracts.connector.ConnectorDescriptor;
import net.java21.data2flow.contracts.connector.ConnectorState;
import net.java21.data2flow.contracts.connector.ConnectorStatus;
import net.java21.data2flow.contracts.connector.PayloadFormat;
import net.java21.data2flow.contracts.connector.RawSink;
import net.java21.data2flow.contracts.connector.ScalingMode;
import net.java21.data2flow.contracts.connector.SourceConfig;
import net.java21.data2flow.contracts.connector.SourceConnector;
import net.java21.data2flow.ingress.connector.domain.DrainableSession;
import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/** 단위 테스트용 커넥터: 열린 세션과 호출을 기록한다. 키 {@code mqtt}로 MQTT 커넥터 자리를 대신한다 */
public final class FakeConnector implements SourceConnector {

    public final List<Session> sessions = new CopyOnWriteArrayList<>();
    public volatile boolean failOpen;

    public final class Session implements DrainableSession {
        public final SourceConfig config;
        public final RawSink sink;
        public final ConnectorContext ctx;
        public volatile boolean started;
        public volatile boolean paused;
        public volatile boolean closed;
        public volatile boolean drained;

        Session(SourceConfig config, RawSink sink, ConnectorContext ctx) {
            this.config = config;
            this.sink = sink;
            this.ctx = ctx;
        }

        @Override
        public void start() {
            started = true;
            ctx.reportStatus(status());
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
        }

        @Override
        public ConnectorStatus status() {
            return ConnectorStatus.of(closed ? ConnectorState.DISCONNECTED : paused ? ConnectorState.DISABLED
                    : started ? ConnectorState.CONNECTED : ConnectorState.DISCONNECTED);
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean drainAndClose(Duration timeout) {
            drained = true;
            close();
            return true;
        }
    }

    @Override
    public ConnectorDescriptor descriptor() {
        return new ConnectorDescriptor("mqtt", "fake", "0.0.1", ConnectorCategory.MQTT, Set.of(AuthMethod.NONE),
                Set.of(PayloadFormat.JSON), AckMode.AFTER_WRITE, ScalingMode.DUAL_ACTIVE, false);
    }

    @Override
    public JsonNode configSchema() {
        return JsonMapper.builder().build().readTree("{\"type\":\"object\"}");
    }

    @Override
    public ConnectionTestResult test(SourceConfig config) {
        if (config.config().has("invalid")) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.valueOf(config.config().get("invalid").asString()),
                    "url");
        }
        return new ConnectionTestResult(List.of(ConnectionTestResult.Step.ok("SUBSCRIBE", 1)), List.of(), false);
    }

    @Override
    public Session open(SourceConfig config, RawSink sink, ConnectorContext ctx) {
        if (failOpen) {
            throw new InvalidSettingsException(InvalidSettingsException.Reason.CONFIG_INVALID, "url");
        }
        Session s = new Session(config, sink, ctx);
        sessions.add(s);
        return s;
    }
}
