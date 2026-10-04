package net.java21.data2flow.ingress.connector.bacnet;

import net.java21.data2flow.contracts.connector.PollCursor;
import net.java21.data2flow.contracts.test.connector.ContractPeer;
import net.java21.data2flow.contracts.test.connector.InMemoryPollCursorStore;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 계약 IT의 상대 BACnet/IP 장치(실제 UDP). 허용 라이선스 BACnet 시뮬레이터가 없어 시험 코드로 만든 최소 장치다(ASHRAE 135 부호화).
 * 장치 객체 이름(ReadProperty Object_Name), 아날로그 입력 Present_Value(실수), 추세 기록 1번의 Log_Buffer(ReadRange bySequenceNumber,
 * 부호 없음 값)를 답한다. 확인 수 = 커넥터가 저장한 기록 번호.
 */
final class BacnetDeviceSimulator implements ContractPeer, AutoCloseable {

    static final int DEVICE = 1234;
    private static final JsonMapper JSON = JsonMapper.builder().build();

    record Rec(long seq, LocalDateTime time, long value) {
    }

    final List<Rec> log = new CopyOnWriteArrayList<>();
    final Map<Integer, Float> analogInputs = new ConcurrentHashMap<>();
    private final DatagramSocket socket;
    private final Thread server;
    private final InMemoryPollCursorStore store;
    private final long sourceId;
    volatile boolean mute;

    BacnetDeviceSimulator(InMemoryPollCursorStore store, long sourceId) throws SocketException {
        this.store = store;
        this.sourceId = sourceId;
        socket = new DatagramSocket(0, InetAddress.getLoopbackAddress());
        server = Thread.ofVirtual().name("bacnet-sim").start(this::serve);
    }

    int port() {
        return socket.getLocalPort();
    }

    /** payload {@code {"device":1234,"log":1,"seq":n,"time":"…","value":v}}를 기록으로 쌓는다(번호는 장치가 매긴다) */
    @Override
    public synchronized void publish(List<byte[]> payloads) throws Exception {
        for (byte[] p : payloads) {
            JsonNode n = JSON.readTree(p);
            log.add(new Rec(log.size() + 1, LocalDateTime.ofInstant(Instant.parse(n.path("time").asString()), ZoneOffset.UTC),
                    n.path("value").asLong()));
        }
    }

    @Override
    public long acknowledgedCount() {
        return store.load(sourceId).map(PollCursor::cursor).map(Long::parseLong).orElse(0L);
    }

    private void serve() {
        byte[] buf = new byte[1500];
        while (!socket.isClosed()) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                socket.receive(p);
                if (mute) {
                    continue;
                }
                byte[] apdu = BacnetCodec.apdu(p.getData(), p.getLength());
                byte[] reply = handle(apdu);
                byte[] frame = BacnetCodec.frame(reply);
                frame[5] = 0x00;   // 응답은 NPDU 응답 기대 없음
                socket.send(new DatagramPacket(frame, frame.length, p.getSocketAddress()));
            } catch (Exception e) {
                if (socket.isClosed()) {
                    return;
                }
            }
        }
    }

    private byte[] handle(byte[] apdu) {
        int invoke = apdu[2] & 0xFF;
        int service = apdu[3] & 0xFF;
        BacnetCodec.Reader r = new BacnetCodec.Reader(apdu, 4);
        long oid = r.next().unsigned();
        int type = (int) (oid >>> 22);
        int instance = (int) (oid & 0x3FFFFF);
        long property = r.next().unsigned();
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        if (service == BacnetCodec.SERVICE_READ_PROPERTY) {
            if (type == BacnetCodec.OBJECT_DEVICE && instance != DEVICE || type == 0 && !analogInputs.containsKey(instance)) {
                return new byte[]{0x50, (byte) invoke, (byte) service, (byte) 0x91, 1, (byte) 0x91, 31};   // object, unknown-object
            }
            o.write(0x30);
            o.write(invoke);
            o.write(service);
            BacnetCodec.contextObjectId(o, 0, type, instance);
            BacnetCodec.contextUnsigned(o, 1, property);
            o.write(0x3E);
            if (type == BacnetCodec.OBJECT_DEVICE) {
                byte[] name = "kit-device".getBytes(StandardCharsets.UTF_8);
                o.write(0x75);
                o.write(name.length + 1);
                o.write(0);
                o.writeBytes(name);
            } else {
                o.write(0x44);
                o.writeBytes(ByteBuffer.allocate(4).putFloat(analogInputs.get(instance)).array());
            }
            o.write(0x3F);
            return o.toByteArray();
        }
        // ReadRange bySequenceNumber
        r.next();                              // [6] 열기
        long from = r.next().unsigned();
        int count = (int) r.next().signed();
        List<Rec> items = log.stream().filter(x -> x.seq() >= from).limit(count).toList();
        o.write(0x30);
        o.write(invoke);
        o.write(service);
        BacnetCodec.contextObjectId(o, 0, type, instance);
        BacnetCodec.contextUnsigned(o, 1, property);
        o.write(0x3A);                         // [3] resultFlags 비트열(길이 2)
        o.write(0x05);
        o.write(0x00);
        BacnetCodec.contextUnsigned(o, 4, items.size());
        o.write(0x5E);                         // [5] 열기
        for (Rec rec : items) {
            o.write(0x0E);
            o.writeBytes(new byte[]{(byte) 0xA4, (byte) (rec.time().getYear() - 1900), (byte) rec.time().getMonthValue(),
                    (byte) rec.time().getDayOfMonth(), (byte) rec.time().getDayOfWeek().getValue()});
            o.writeBytes(new byte[]{(byte) 0xB4, (byte) rec.time().getHour(), (byte) rec.time().getMinute(),
                    (byte) rec.time().getSecond(), 0});
            o.write(0x0F);
            o.write(0x1E);
            BacnetCodec.contextUnsigned(o, 4, rec.value());
            o.write(0x1F);
        }
        o.write(0x5F);
        if (!items.isEmpty()) {
            BacnetCodec.contextUnsigned(o, 6, items.get(0).seq());
        }
        return o.toByteArray();
    }

    @Override
    public void close() {
        socket.close();
        server.interrupt();
    }
}
