package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import org.junit.jupiter.api.Test;

final class HttpMessagePackFlowCreateCodecTest {
    @Test
    void writesCreateManyWithoutChangingItsTypedWireContract() throws Exception {
        FlowCreateCodecTestSupport.Fixture fixture = FlowCreateCodecTestSupport.fixture();

        byte[] encoded =
                HttpMessagePackCodec.encode(
                        output ->
                                HttpMessagePackFlowCreateCodec.writeCommand(
                                        output, fixture.batch()));
        Map<String, Object> descriptor = HttpMessagePackCodec.decode(encoded);

        assertEquals("FLOW.CREATE_MANY", descriptor.get("command"));
        assertEquals(0x020FL, descriptor.get("opcode"));
        Map<?, ?> payload = assertInstanceOf(Map.class, descriptor.get("payload"));
        FlowCreateCodecTestSupport.assertPayload(payload, fixture);
    }
}
