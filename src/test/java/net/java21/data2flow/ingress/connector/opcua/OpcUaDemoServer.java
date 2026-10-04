package net.java21.data2flow.ingress.connector.opcua;

import net.java21.data2flow.contracts.test.connector.ContractPeer;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.util.EndpointUtil;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 계약 IT의 상대: Eclipse Milo OPC UA 데모 서버 컨테이너({@code ghcr.io/digitalpetri/opc-ua-demo-server}, 시험 전용). 문자열 노드
 * {@code ns=2;s=CTT.Static.AllProfiles.Scalar.String}에 값을 쓴다. OPC UA 구독은 표본(sampling)이라 짧은 사이에 여러 번 바뀌면 중간 값이
 * 합쳐진다(IEC 62541-4 §5.12). 그래서 실제 장비처럼 시험 상대 자신의 구독에 값이 보인 뒤에 다음 값을 쓴다. 확인 수는 없다(-1).
 */
final class OpcUaDemoServer implements ContractPeer, AutoCloseable {

    static final String NODE = "ns=2;s=CTT.Static.AllProfiles.Scalar.String";
    final GenericContainer<?> container = new GenericContainer<>("ghcr.io/digitalpetri/opc-ua-demo-server:latest")
            .withExposedPorts(4840).waitingFor(Wait.forLogMessage(".*Demo Server.*started.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    private OpcUaClient writer;
    private final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<>();

    void start() throws Exception {
        container.start();
        writer = OpcUaClient.create(endpointUrl(), eps -> eps.stream()
                        .filter(e -> e.getSecurityPolicyUri().endsWith("#None")).findFirst()
                        .map(e -> EndpointUtil.updateUrl(e, container.getHost(), container.getMappedPort(4840))),
                t -> { }, b -> { });
        writer.connect();
        OpcUaSubscription sub = new OpcUaSubscription(writer, 5.0);
        sub.create();
        OpcUaMonitoredItem item = OpcUaMonitoredItem.newDataItem(NodeId.parse(NODE));
        item.setSamplingInterval(0);
        item.setQueueSize(UInteger.valueOf(1000));
        item.setDataValueListener((it, dv) -> seen.add(String.valueOf(dv.getValue().getValue())));
        sub.addMonitoredItem(item);
        sub.synchronizeMonitoredItems();
    }

    String endpointUrl() {
        return "opc.tcp://" + container.getHost() + ":" + container.getMappedPort(4840) + "/milo";
    }

    @Override
    public void publish(List<byte[]> payloads) throws Exception {
        for (byte[] p : payloads) {
            String v = new String(p, StandardCharsets.UTF_8);
            List<StatusCode> sc = writer.writeValues(List.of(NodeId.parse(NODE)), List.of(DataValue.valueOnly(new Variant(v))));
            if (sc.get(0).isBad()) {
                throw new IllegalStateException("쓰기 실패: " + sc.get(0));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            String got;
            do {
                got = seen.poll(Math.max(1, (deadline - System.nanoTime()) / 1_000_000L), TimeUnit.MILLISECONDS);
            } while (got != null && !got.equals(v));
        }
    }

    @Override
    public long acknowledgedCount() {
        return -1;
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.disconnect();
        }
        container.stop();
    }
}
