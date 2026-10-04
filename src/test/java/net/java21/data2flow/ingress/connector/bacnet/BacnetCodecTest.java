package net.java21.data2flow.ingress.connector.bacnet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.14 BACnet 최소 부호화가 ASHRAE 135 Annex F 예(ReadProperty AI 5 Present_Value = 72.4)와 같은 바이트를 만들고 읽는다 */
class BacnetCodecTest {

    private static final HexFormat HEX = HexFormat.of();

    @Test
    @DisplayName("DSC-09.14 Annex F.3.5 ReadProperty 요청 바이트")
    void readPropertyRequest() {
        assertThat(HEX.formatHex(BacnetCodec.readProperty(1, 0, 5, 85))).isEqualTo("0005010c0c0000000519" + "55");
        byte[] frame = BacnetCodec.frame(BacnetCodec.readProperty(1, 0, 5, 85));
        assertThat(HEX.formatHex(frame)).startsWith("810a0011" + "0104");
    }

    @Test
    @DisplayName("DSC-09.14 Annex F.3.5 ReadProperty-ACK 72.4 읽기, Error·Reject·Abort는 예외")
    void readPropertyAck() {
        byte[] ack = HEX.parseHex("30010c0c00000005195" + "53e444290cccd3f");
        assertThat((Double) BacnetCodec.readPropertyValue(ack)).isCloseTo(72.4, org.assertj.core.data.Offset.offset(1e-5));
        assertThatThrownBy(() -> BacnetCodec.readPropertyValue(HEX.parseHex("50010c9101911f")))
                .hasMessageContaining("class=1 code=31");
        assertThatThrownBy(() -> BacnetCodec.readPropertyValue(HEX.parseHex("600102"))).hasMessageContaining("Reject");
        assertThatThrownBy(() -> BacnetCodec.readPropertyValue(HEX.parseHex("700104"))).hasMessageContaining("Abort");
    }

    @Test
    @DisplayName("DSC-09.14 ReadRange bySequenceNumber 요청: [6] 열기, Unsigned 기준 번호, INTEGER 개수, [6] 닫기")
    void readRangeRequest() {
        assertThat(HEX.formatHex(BacnetCodec.readRangeBySequence(7, 20, 1, 300, 50)))
                .isEqualTo("0005071a" + "0c05000001" + "198" + "3" + "6e" + "22012c" + "3132" + "6f");
    }

    @Test
    @DisplayName("DSC-09.14 BVLC가 아니거나 네트워크 계층 메시지면 거부, NPDU 라우팅 정보는 건너뛴다")
    void npdu() {
        assertThatThrownBy(() -> BacnetCodec.apdu(new byte[]{0, 0, 0, 0, 1, 0}, 6)).hasMessageContaining("BVLC");
        byte[] routed = HEX.parseHex("810a000e" + "0120" + "0001" + "01" + "05" + "ff" + "300100");
        assertThat(HEX.formatHex(BacnetCodec.apdu(routed, routed.length))).isEqualTo("300100");
    }
}
