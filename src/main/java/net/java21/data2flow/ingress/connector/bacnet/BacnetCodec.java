package net.java21.data2flow.ingress.connector.bacnet;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * BACnet/IP 최소 부호화(ASHRAE 135-2020 §20 APDU 부호화, Annex J BVLL). 허용 라이선스 라이브러리가 없어(BACnet4J는 GPL-3.0, ADR-018)
 * 필요한 서비스만 직접 구현한다: ReadProperty(§15.5)와 ReadRange bySequenceNumber(§15.8, 추세 기록 Log_Buffer). 쓰기·COV는 없다.
 */
public final class BacnetCodec {

    public static final int PORT = 47808;
    public static final int SERVICE_READ_PROPERTY = 12;
    public static final int SERVICE_READ_RANGE = 26;
    public static final int PROP_PRESENT_VALUE = 85;
    public static final int PROP_LOG_BUFFER = 131;
    public static final int PROP_OBJECT_NAME = 77;
    public static final int OBJECT_DEVICE = 8;
    public static final int OBJECT_TREND_LOG = 20;

    private BacnetCodec() {
    }

    // ---- 요청 만들기 ----

    /** BVLC Original-Unicast-NPDU + NPDU(응답 기대) + APDU */
    public static byte[] frame(byte[] apdu) {
        int len = 4 + 2 + apdu.length;
        ByteBuffer b = ByteBuffer.allocate(len);
        b.put((byte) 0x81).put((byte) 0x0A).putShort((short) len);
        b.put((byte) 0x01).put((byte) 0x04);
        b.put(apdu);
        return b.array();
    }

    /** ReadProperty 요청 APDU(§15.5.1.1). 예: AI 5 Present_Value → 00 05 01 0C 0C 00000005 19 55 */
    public static byte[] readProperty(int invokeId, int objectType, int instance, int property) {
        ByteArrayOutputStream o = confirmedHeader(invokeId, SERVICE_READ_PROPERTY);
        contextObjectId(o, 0, objectType, instance);
        contextUnsigned(o, 1, property);
        return o.toByteArray();
    }

    /** ReadRange bySequenceNumber 요청 APDU(§15.8.1.1): 기준 번호부터 {@code count}개 */
    public static byte[] readRangeBySequence(int invokeId, int objectType, int instance, long fromSequence, int count) {
        ByteArrayOutputStream o = confirmedHeader(invokeId, SERVICE_READ_RANGE);
        contextObjectId(o, 0, objectType, instance);
        contextUnsigned(o, 1, PROP_LOG_BUFFER);
        o.write(0x6E);   // [6] 열기 bySequenceNumber
        appUnsigned(o, fromSequence);
        appSigned(o, count);
        o.write(0x6F);   // [6] 닫기
        return o.toByteArray();
    }

    private static ByteArrayOutputStream confirmedHeader(int invokeId, int service) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(0x00);           // Confirmed-Request, 분할 없음
        o.write(0x05);           // 최대 APDU 1476
        o.write(invokeId & 0xFF);
        o.write(service);
        return o;
    }

    static void contextObjectId(ByteArrayOutputStream o, int tag, int type, int instance) {
        o.write((tag << 4) | 0x08 | 4);
        writeInt(o, ((long) type << 22) | (instance & 0x3FFFFF), 4);
    }

    static void contextUnsigned(ByteArrayOutputStream o, int tag, long value) {
        int n = unsignedLength(value);
        o.write((tag << 4) | 0x08 | n);
        writeInt(o, value, n);
    }

    static void appUnsigned(ByteArrayOutputStream o, long value) {
        int n = unsignedLength(value);
        o.write((2 << 4) | n);
        writeInt(o, value, n);
    }

    static void appSigned(ByteArrayOutputStream o, long value) {
        int n = value >= -128 && value <= 127 ? 1 : value >= -32768 && value <= 32767 ? 2 : value >= -8388608 && value <= 8388607 ? 3 : 4;
        o.write((3 << 4) | n);
        writeInt(o, value, n);
    }

    static int unsignedLength(long v) {
        return v < 0x100 ? 1 : v < 0x10000 ? 2 : v < 0x1000000 ? 3 : 4;
    }

    static void writeInt(ByteArrayOutputStream o, long v, int n) {
        for (int i = n - 1; i >= 0; i--) {
            o.write((int) (v >>> (8 * i)) & 0xFF);
        }
    }

    // ---- 응답 읽기 ----

    /** BVLC·NPDU를 벗기고 APDU를 돌려준다 */
    public static byte[] apdu(byte[] datagram, int length) {
        if (length < 6 || (datagram[0] & 0xFF) != 0x81) {
            throw new BacnetException("BVLC가 아닙니다");
        }
        int i = 4;
        int version = datagram[i++] & 0xFF;
        int control = datagram[i++] & 0xFF;
        if (version != 1) {
            throw new BacnetException("NPDU 버전 " + version);
        }
        if ((control & 0x80) != 0) {
            throw new BacnetException("네트워크 계층 메시지");
        }
        if ((control & 0x20) != 0) {   // DNET 있음
            i += 2;
            int dlen = datagram[i++] & 0xFF;
            i += dlen;
        }
        if ((control & 0x08) != 0) {   // SNET 있음
            i += 2;
            int slen = datagram[i++] & 0xFF;
            i += slen;
        }
        if ((control & 0x20) != 0) {
            i++;   // hop count
        }
        byte[] apdu = new byte[length - i];
        System.arraycopy(datagram, i, apdu, 0, apdu.length);
        return apdu;
    }

    /** 응답 APDU의 invoke ID */
    public static int invokeId(byte[] apdu) {
        return apdu[1] & 0xFF;
    }

    /** ComplexACK가 아니면(Error·Reject·Abort) 예외 */
    static Reader complexAck(byte[] apdu, int service) {
        int type = (apdu[0] & 0xF0) >> 4;
        switch (type) {
            case 3 -> {
                if ((apdu[0] & 0x08) != 0) {
                    throw new BacnetException("분할 응답은 지원하지 않습니다");
                }
                if ((apdu[2] & 0xFF) != service) {
                    throw new BacnetException("다른 서비스 응답: " + (apdu[2] & 0xFF));
                }
                return new Reader(apdu, 3);
            }
            case 5 -> {
                Reader r = new Reader(apdu, 3);
                long cls = r.next().unsigned();
                long code = r.next().unsigned();
                throw new BacnetException("BACnet Error class=" + cls + " code=" + code);
            }
            case 6 -> throw new BacnetException("BACnet Reject reason=" + (apdu[2] & 0xFF));
            case 7 -> throw new BacnetException("BACnet Abort reason=" + (apdu[2] & 0xFF));
            default -> throw new BacnetException("예상하지 않은 PDU " + type);
        }
    }

    /** ReadProperty-ACK의 값(§15.5.1.3). 실수·정수·불린·열거·문자열을 Java 값으로 */
    public static Object readPropertyValue(byte[] apdu) {
        Reader r = complexAck(apdu, SERVICE_READ_PROPERTY);
        r.next();                 // [0] 객체
        r.next();                 // [1] 속성
        Tag t = r.next();
        if (t.context && t.number == 2) {
            t = r.next();         // [2] 배열 색인
        }
        if (!t.opening || t.number != 3) {
            throw new BacnetException("propertyValue가 없습니다");
        }
        return r.next().value();
    }

    /** 추세 기록 항목 */
    public record LogRecord(long sequence, LocalDateTime timestamp, Object value) {
    }

    /** ReadRange-ACK의 기록들(§15.8.1.3). firstSequenceNumber로 번호를 매긴다 */
    public static List<LogRecord> readRangeRecords(byte[] apdu) {
        Reader r = complexAck(apdu, SERVICE_READ_RANGE);
        List<Object[]> items = new ArrayList<>();
        Long first = null;
        while (r.hasMore()) {
            Tag t = r.next();
            if (t.opening && t.number == 5) {
                while (true) {
                    Tag x = r.next();
                    if (x.closing && x.number == 5) {
                        break;
                    }
                    if (!(x.opening && x.number == 0)) {
                        throw new BacnetException("LogRecord timestamp가 없습니다");
                    }
                    Tag date = r.next();
                    Tag time = r.next();
                    r.next();                       // [0] 닫기
                    Tag datumOpen = r.next();       // [1] 열기
                    if (!(datumOpen.opening && datumOpen.number == 1)) {
                        throw new BacnetException("logDatum이 없습니다");
                    }
                    Tag datum = r.next();
                    r.next();                       // [1] 닫기
                    if (r.peekContext(2)) {
                        r.next();                   // [2] statusFlags
                    }
                    items.add(new Object[]{date.dateTime(time), datum.logDatum()});
                }
            } else if (t.context && t.number == 6) {
                first = t.unsigned();
            }
        }
        List<LogRecord> out = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            out.add(new LogRecord(first == null ? -1 : first + i, (LocalDateTime) items.get(i)[0], items.get(i)[1]));
        }
        return out;
    }

    /** 태그 하나 */
    public record Tag(int number, boolean context, boolean opening, boolean closing, byte[] data) {

        long unsigned() {
            long v = 0;
            for (byte b : data) {
                v = (v << 8) | (b & 0xFF);
            }
            return v;
        }

        long signed() {
            long v = data.length > 0 && data[0] < 0 ? -1 : 0;
            for (byte b : data) {
                v = (v << 8) | (b & 0xFF);
            }
            return v;
        }

        /** 응용 태그 값 */
        Object value() {
            if (context) {
                throw new BacnetException("응용 태그가 아닙니다");
            }
            return switch (number) {
                case 0 -> null;
                case 1 -> data.length == 0 ? Boolean.FALSE : Boolean.TRUE;   // 응용 Boolean은 길이 칸이 값(Reader가 처리)
                case 2 -> unsigned();
                case 3 -> signed();
                case 4 -> (double) ByteBuffer.wrap(data).getFloat();
                case 5 -> ByteBuffer.wrap(data).getDouble();
                case 7 -> new String(data, 1, data.length - 1, data[0] == 0 ? java.nio.charset.StandardCharsets.UTF_8
                        : java.nio.charset.StandardCharsets.ISO_8859_1);
                case 9 -> unsigned();
                default -> java.util.HexFormat.of().formatHex(data);
            };
        }

        /** logDatum 선택지(§21 BACnetLogRecord): [1] 불린 [2] 실수 [3] 열거 [4] 부호 없음 [5] 부호 있음 */
        Object logDatum() {
            return switch (number) {
                case 1 -> unsigned() != 0;
                case 2 -> (double) ByteBuffer.wrap(data).getFloat();
                case 3, 4 -> unsigned();
                case 5 -> signed();
                case 7 -> null;
                default -> java.util.HexFormat.of().formatHex(data);
            };
        }

        LocalDateTime dateTime(Tag time) {
            byte[] d = data;
            byte[] t = time.data;
            return LocalDateTime.of(1900 + (d[0] & 0xFF), d[1] & 0xFF, d[2] & 0xFF, t[0] & 0xFF, t[1] & 0xFF, t[2] & 0xFF,
                    (t[3] & 0xFF) == 0xFF ? 0 : (t[3] & 0xFF) * 10_000_000);
        }
    }

    /** 태그 순서대로 읽기(§20.2.1) */
    static final class Reader {
        private final byte[] b;
        private int i;

        Reader(byte[] b, int offset) {
            this.b = b;
            this.i = offset;
        }

        boolean hasMore() {
            return i < b.length;
        }

        boolean peekContext(int number) {
            return hasMore() && (b[i] & 0x08) != 0 && ((b[i] & 0xF0) >> 4) == number && (b[i] & 0x07) < 6;
        }

        Tag next() {
            if (!hasMore()) {
                throw new BacnetException("APDU가 끝났습니다");
            }
            int first = b[i++] & 0xFF;
            int number = (first & 0xF0) >> 4;
            boolean context = (first & 0x08) != 0;
            int lvt = first & 0x07;
            if (number == 15) {
                number = b[i++] & 0xFF;
            }
            if (context && lvt == 6) {
                return new Tag(number, true, true, false, new byte[0]);
            }
            if (context && lvt == 7) {
                return new Tag(number, true, false, true, new byte[0]);
            }
            if (!context && number == 1) {   // 응용 Boolean: 값이 길이 칸에
                return new Tag(1, false, false, false, lvt == 0 ? new byte[0] : new byte[]{1});
            }
            int len = lvt;
            if (lvt == 5) {
                len = b[i++] & 0xFF;
                if (len == 254) {
                    len = ((b[i] & 0xFF) << 8) | (b[i + 1] & 0xFF);
                    i += 2;
                }
            }
            byte[] data = new byte[len];
            System.arraycopy(b, i, data, 0, len);
            i += len;
            return new Tag(number, context, false, false, data);
        }
    }

    /** 부호화·응답 오류 */
    public static class BacnetException extends RuntimeException {
        public BacnetException(String message) {
            super(message);
        }
    }
}
