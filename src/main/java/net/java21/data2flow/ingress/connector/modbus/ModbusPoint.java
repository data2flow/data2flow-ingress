package net.java21.data2flow.ingress.connector.modbus;

import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Locale;

/**
 * Modbus 측정점 하나(DSC-05.01, AT-DSC-11.1). 함수(코일·입력 접점·입력 레지스터·보유 레지스터), 0부터 세는 주소, 자료형, 단어 순서,
 * 배율·오프셋. Modicon 표기({@code 40001} = 보유 레지스터 0번)도 받는다.
 *
 * @param name     값 이름(payload {@code values} 키)
 * @param function COIL, DISCRETE, INPUT, HOLDING
 * @param address  0부터 세는 주소
 * @param type     BOOL, INT16, UINT16, INT32, UINT32, FLOAT32
 * @param swap     32비트 값의 단어 순서가 낮은 단어 먼저(LITTLE)
 * @param scale    배율(값 × scale + offset)
 * @param offset   오프셋
 */
public record ModbusPoint(String name, String function, int address, String type, boolean swap, double scale, double offset) {

    public static ModbusPoint of(String name, String register, String function, Integer address, String type, String wordOrder,
                                 double scale, double offset) {
        String fn;
        int addr;
        if (register != null) {   // Modicon: 0xxxx 코일, 1xxxx 입력 접점, 3xxxx 입력 레지스터, 4xxxx 보유 레지스터
            if (!register.matches("[0134]\\d{4,5}")) {
                throw InvalidSettingsException.config("register");
            }
            int n = Integer.parseInt(register);
            int base = register.length() == 6 ? 100_000 : 10_000;
            fn = switch (register.charAt(0)) {
                case '0' -> "COIL";
                case '1' -> "DISCRETE";
                case '3' -> "INPUT";
                default -> "HOLDING";
            };
            addr = n % base - 1;
            if (addr < 0) {
                throw InvalidSettingsException.config("register");
            }
        } else {
            fn = function == null ? "HOLDING" : function.toUpperCase(Locale.ROOT);
            if (address == null || address < 0 || address > 65535) {
                throw InvalidSettingsException.config("address");
            }
            addr = address;
        }
        String t = type == null ? (fn.equals("COIL") || fn.equals("DISCRETE") ? "BOOL" : "UINT16") : type.toUpperCase(Locale.ROOT);
        if (!java.util.Set.of("COIL", "DISCRETE", "INPUT", "HOLDING").contains(fn)
                || !java.util.Set.of("BOOL", "INT16", "UINT16", "INT32", "UINT32", "FLOAT32").contains(t)
                || (t.equals("BOOL")) != (fn.equals("COIL") || fn.equals("DISCRETE"))) {
            throw InvalidSettingsException.config("type");
        }
        return new ModbusPoint(name, fn, addr, t, "LITTLE".equalsIgnoreCase(wordOrder), scale, offset);
    }

    /** 레지스터 몇 개를 읽는가 */
    public int width() {
        return switch (type) {
            case "INT32", "UINT32", "FLOAT32" -> 2;
            default -> 1;
        };
    }

    /** 레지스터 값(부호 없는 16비트)들을 값으로. 배율·오프셋 적용(AT-DSC-11.1: FLOAT32 225 × 0.1 = 22.5) */
    public Number decode(int[] words) {
        long raw;
        double value = switch (type) {
            case "INT16" -> (short) words[0];
            case "UINT16" -> words[0] & 0xFFFF;
            case "INT32" -> {
                raw = combine(words);
                yield (int) raw;
            }
            case "UINT32" -> combine(words) & 0xFFFFFFFFL;
            case "FLOAT32" -> Float.intBitsToFloat((int) combine(words));
            default -> throw new IllegalStateException(type);
        };
        double scaled = value * scale + offset;
        if (scale == 1 && offset == 0 && !type.equals("FLOAT32")) {
            return (long) scaled;
        }
        return new BigDecimal(Double.toString(scaled)).round(MathContext.DECIMAL64).stripTrailingZeros().doubleValue();
    }

    private long combine(int[] w) {
        int hi = swap ? w[1] : w[0];
        int lo = swap ? w[0] : w[1];
        return ((long) (hi & 0xFFFF) << 16) | (lo & 0xFFFF);
    }
}
