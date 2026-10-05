package net.java21.data2flow.ingress.payload.domain;

import net.java21.data2flow.ingress.payload.codec.PayloadDecodeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;

/**
 * payload 압축 해제(DSC-09.07, AT-DSC-16.2). 풀린 크기가 한도를 넘으면 멈추고 오류로 본다(압축 폭탄 방지).
 * deflate는 zlib 머리(RFC 1950)가 있으면 그것으로, 없으면 원시 deflate(RFC 1951)로 읽는다.
 */
public enum Compression {
    NONE, GZIP, DEFLATE;

    public static Compression of(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        return switch (value.strip().toUpperCase(Locale.ROOT)) {
            case "NONE" -> NONE;
            case "GZIP" -> GZIP;
            case "DEFLATE", "ZLIB" -> DEFLATE;
            default -> throw new IllegalArgumentException("지원하지 않는 압축: " + value);
        };
    }

    /**
     * @param maxBytes 풀린 바이트 한도
     */
    public byte[] decompress(byte[] payload, int maxBytes) throws PayloadDecodeException {
        return switch (this) {
            case NONE -> payload;
            case GZIP -> gzip(payload, maxBytes);
            case DEFLATE -> deflate(payload, maxBytes);
        };
    }

    private static byte[] gzip(byte[] payload, int maxBytes) throws PayloadDecodeException {
        try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(payload))) {
            return readLimited(in, maxBytes);
        } catch (IOException e) {
            throw new PayloadDecodeException("gzip을 풀 수 없습니다: " + e.getMessage(), e);
        }
    }

    private static byte[] deflate(byte[] payload, int maxBytes) throws PayloadDecodeException {
        boolean zlib = payload.length >= 2 && (payload[0] & 0x0F) == 8 && ((payload[0] & 0xFF) << 8 | payload[1] & 0xFF) % 31 == 0;
        Inflater inflater = new Inflater(!zlib);
        try {
            inflater.setInput(payload);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            while (!inflater.finished()) {
                int n = inflater.inflate(buf);
                if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                    throw new PayloadDecodeException("deflate 데이터가 중간에 끝났습니다");
                }
                out.write(buf, 0, n);
                if (out.size() > maxBytes) {
                    throw new PayloadDecodeException("압축을 푼 크기가 " + maxBytes + "바이트를 넘습니다");
                }
            }
            return out.toByteArray();
        } catch (DataFormatException e) {
            throw new PayloadDecodeException("deflate를 풀 수 없습니다: " + e.getMessage(), e);
        } finally {
            inflater.end();
        }
    }

    private static byte[] readLimited(InputStream in, int maxBytes) throws IOException, PayloadDecodeException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
            if (out.size() > maxBytes) {
                throw new PayloadDecodeException("압축을 푼 크기가 " + maxBytes + "바이트를 넘습니다");
            }
        }
        return out.toByteArray();
    }
}
