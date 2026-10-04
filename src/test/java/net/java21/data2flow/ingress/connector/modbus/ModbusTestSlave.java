package net.java21.data2flow.ingress.connector.modbus;

import com.ghgande.j2mod.modbus.procimg.SimpleInputRegister;
import com.ghgande.j2mod.modbus.procimg.SimpleProcessImage;
import com.ghgande.j2mod.modbus.procimg.SimpleRegister;
import com.ghgande.j2mod.modbus.slave.ModbusSlave;
import com.ghgande.j2mod.modbus.slave.ModbusSlaveFactory;
import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;

/**
 * 계약 IT의 상대 Modbus TCP 장비(j2mod 슬레이브, 실제 TCP). 보유 레지스터 0~1 = 기록 수(UINT32), 100번부터 기록 4개 단어 × 고리 2,000칸.
 * 0번 단위의 보유 레지스터 40001~40002에는 FLOAT32 225.0(AT-DSC-11.1), 입력 레지스터 30001에는 INT16 -40을 둔다.
 * 확인 수 = 커넥터가 저장한 위치(기록 번호).
 */
final class ModbusTestSlave implements ContractPeer, AutoCloseable {

    static final int RING = 2000;
    static final int RECORD = 4;
    static final int RECORD_BASE = 100;
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final SimpleProcessImage image = new SimpleProcessImage(1);
    private final InMemoryPollCursorStore store;
    private final long sourceId;
    final int port;
    private ModbusSlave slave;
    private long count;

    ModbusTestSlave(InMemoryPollCursorStore store, long sourceId) throws Exception {
        this.store = store;
        this.sourceId = sourceId;
        for (int i = 0; i < RECORD_BASE + RING * RECORD; i++) {
            image.addRegister(new SimpleRegister(0));
        }
        image.addInputRegister(new SimpleInputRegister(-40));
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        setFloat(10, 225.0f);
        start();
    }

    /** 보유 레지스터 {@code address}·{@code address+1}에 FLOAT32(큰 단어 먼저) */
    void setFloat(int address, float v) {
        int bits = Float.floatToIntBits(v);
        image.getRegister(address).setValue((bits >>> 16) & 0xFFFF);
        image.getRegister(address + 1).setValue(bits & 0xFFFF);
    }

    void start() throws Exception {
        slave = ModbusSlaveFactory.createTCPSlave(InetAddress.getLoopbackAddress(), port, 5, false);
        slave.addProcessImage(1, image);
        slave.open();
    }

    /** 장비 전원 끔(연결 끊김) */
    void stop() {
        ModbusSlaveFactory.close(slave);
    }

    /** payload {@code {"unitId":1,"registers":[a,b,c,d]}}를 기록 하나로 쌓는다 */
    @Override
    public synchronized void publish(List<byte[]> payloads) throws Exception {
        for (byte[] p : payloads) {
            JsonNode regs = JSON.readTree(p).path("registers");
            int slot = (int) (count % RING);
            for (int i = 0; i < RECORD; i++) {
                image.getRegister(RECORD_BASE + slot * RECORD + i).setValue(regs.get(i).asInt());
            }
            count++;
            image.getRegister(0).setValue((int) (count >>> 16) & 0xFFFF);
            image.getRegister(1).setValue((int) count & 0xFFFF);
        }
    }

    @Override
    public long acknowledgedCount() {
        return store.load(sourceId).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
    }

    @Override
    public void close() {
        stop();
    }
}
