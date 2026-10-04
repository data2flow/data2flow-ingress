package net.java21.data2flow.ingress.connector.mqttpreset;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.ingress.connector.mqtt.MqttSourceConnector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.02 TC-DSC-241: Sparkplug B 커넥터가 시험 Mosquitto를 상대로 계약 키트 공통 시나리오를 통과한다. payload는 Sparkplug B Protobuf
 * (Payload{timestamp, metrics[{name, datatype=String, string_value}], seq})를 직접 인코딩해 보낸다(Tahu 없이 같은 바이트). STATE·NCMD는 기록하지
 * 않는다.
 */
class SparkplugBConnectorContractIT extends AbstractMqttPresetContractIT {

    private long seq;

    @Override
    protected MqttPresetConnector preset(MqttSourceConnector mqtt) {
        return MqttPresets.sparkplug(mqtt);
    }

    @Override
    protected ObjectNode presetConfig(String proxyUrl) {
        return JSON.createObjectNode().put("url", proxyUrl).put("groupId", "kitgroup");
    }

    @Override
    protected String publishTopic() {
        return "spBv1.0/kitgroup/DDATA/edge1/sensor1";
    }

    @Override
    protected long sourceId() {
        return 40;
    }

    /** Sparkplug B Payload(Protobuf): 측정값 하나(name=kit, String)를 담는다 */
    @Override
    protected List<byte[]> payloads(int count) {
        List<byte[]> json = super.payloads(count);
        List<byte[]> out = new ArrayList<>(count);
        for (byte[] j : json) {
            out.add(sparkplugPayload(j, seq++ % 256));
        }
        return out;
    }

    static byte[] sparkplugPayload(byte[] value, long seq) {
        ByteArrayOutputStream metric = new ByteArrayOutputStream();
        field(metric, 1, "kit".getBytes(StandardCharsets.UTF_8));   // name
        varintField(metric, 4, 12);                                 // datatype String
        field(metric, 15, value);                                   // string_value
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        varintField(payload, 1, 1_790_000_000_000L);                // timestamp
        field(payload, 2, metric.toByteArray());                    // metrics
        varintField(payload, 3, seq);                               // seq
        return payload.toByteArray();
    }

    private static void field(ByteArrayOutputStream out, int number, byte[] bytes) {
        varint(out, ((long) number << 3) | 2);
        varint(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void varintField(ByteArrayOutputStream out, int number, long value) {
        varint(out, (long) number << 3);
        varint(out, value);
    }

    private static void varint(ByteArrayOutputStream out, long v) {
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    @Test
    @DisplayName("DSC-09.02 Sparkplug STATE·NCMD는 기록하지 않고 확인만, NBIRTH·DDATA는 기록한다")
    void ignoresStateAndCommands() {
        startAndAwaitConnected();
        byte[] birth = sparkplugPayload("{\"birth\":1}".getBytes(StandardCharsets.UTF_8), 0);
        publisher().publish("spBv1.0/kitgroup/NCMD/edge1", "cmd".getBytes(StandardCharsets.UTF_8));
        publisher().publish("spBv1.0/kitgroup/NBIRTH/edge1", birth);
        await().until(() -> sink().written().stream().anyMatch(e -> e.topic().endsWith("NBIRTH/edge1")));
        assertThat(sink().written()).extracting(RawEnvelope::topic).noneMatch(t -> t.contains("NCMD"));
        assertThat(MqttPresets.sparkplugData("spBv1.0/STATE/host1")).isFalse();
        assertThat(MqttPresets.sparkplugData("spBv1.0/g/DDATA/e/d")).isTrue();
    }
}
