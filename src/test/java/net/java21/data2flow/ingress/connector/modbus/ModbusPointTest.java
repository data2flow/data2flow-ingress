package net.java21.data2flow.ingress.connector.modbus;

import net.java21.data2flow.ingress.connector.mqtt.InvalidSettingsException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-05.01 TC-DSC-131 AT-DSC-11.1: Modbus 측정점 주소·자료형·배율 해석 */
class ModbusPointTest {

    @Test
    @DisplayName("DSC-05.01 AT-DSC-11.1 FLOAT32 레지스터 40001, 배율 0.1, 읽기 값 225 → 22.5")
    void float32Scaled() {
        ModbusPoint p = ModbusPoint.of("temp", "40001", null, null, "FLOAT32", "BIG", 0.1, 0);
        assertThat(p.function()).isEqualTo("HOLDING");
        assertThat(p.address()).isZero();
        assertThat(p.width()).isEqualTo(2);
        int bits = Float.floatToIntBits(225.0f);
        assertThat(p.decode(new int[]{bits >>> 16, bits & 0xFFFF})).isEqualTo(22.5);
        ModbusPoint little = ModbusPoint.of("temp", "40001", null, null, "FLOAT32", "LITTLE", 0.1, 0);
        assertThat(little.decode(new int[]{bits & 0xFFFF, bits >>> 16})).isEqualTo(22.5);
    }

    @Test
    @DisplayName("DSC-05.01 Modicon 표기와 자료형: 30001 INT16 -40, 400001(6자리), UINT32·INT32, 코일·접점은 BOOL만")
    void notationsAndTypes() {
        assertThat(ModbusPoint.of("o", "30001", null, null, "INT16", null, 1, 0).decode(new int[]{0xFFD8})).isEqualTo(-40L);
        assertThat(ModbusPoint.of("x", "400010", null, null, null, null, 1, 0).address()).isEqualTo(9);
        assertThat(ModbusPoint.of("u", null, "INPUT", 5, "UINT32", null, 1, 0).decode(new int[]{0x0001, 0x0000})).isEqualTo(65536L);
        assertThat(ModbusPoint.of("s", null, "HOLDING", 5, "INT32", null, 1, 0).decode(new int[]{0xFFFF, 0xFFFE})).isEqualTo(-2L);
        assertThat(ModbusPoint.of("c", "00001", null, null, null, null, 1, 0).type()).isEqualTo("BOOL");
        assertThat(ModbusPoint.of("v", null, "HOLDING", 1, "UINT16", null, 2, 1).decode(new int[]{10})).isEqualTo(21.0);
        assertThatThrownBy(() -> ModbusPoint.of("c", "00001", null, null, "INT16", null, 1, 0))
                .isInstanceOf(InvalidSettingsException.class);
        assertThatThrownBy(() -> ModbusPoint.of("c", "20001", null, null, null, null, 1, 0)).isInstanceOf(InvalidSettingsException.class);
        assertThatThrownBy(() -> ModbusPoint.of("c", null, "HOLDING", -1, null, null, 1, 0)).isInstanceOf(InvalidSettingsException.class);
    }
}
