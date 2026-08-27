package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FlowCommandEncoderTest {
    @Test
    void preparesCreateWithNamedValuesAsTypedPayload() {
        byte[] value = {1, 2, 3};

        FlowCommandEncoder.Prepared prepared =
                FlowCommandEncoder.prepare(
                        "FLOW.CREATE",
                        List.of(
                                "flow-1",
                                "TYPE",
                                "order",
                                "STATE",
                                "queued",
                                "VALUE",
                                "document",
                                value,
                                "VALUE_REF",
                                "shared",
                                "ref-1",
                                "IDEMPOTENT",
                                "true"));

        assertEquals(0x0201, prepared.opcode());
        assertEquals("flow-1", prepared.payload().get("id"));
        assertEquals(Map.of("document", value), prepared.payload().get("values"));
        assertEquals(Map.of("shared", "ref-1"), prepared.payload().get("value_refs"));
        assertEquals(true, prepared.payload().get("idempotent"));
    }

    @Test
    void preparesOwnedValuePutAndTerminalNamedValueMutations() {
        FlowCommandEncoder.Prepared put =
                FlowCommandEncoder.prepare(
                        "FLOW.VALUE.PUT",
                        List.of(
                                new byte[] {9},
                                "OWNER_FLOW_ID",
                                "flow-1",
                                "NAME",
                                "document",
                                "OVERRIDE",
                                "true",
                                "NOW",
                                100L));
        FlowCommandEncoder.Prepared complete =
                FlowCommandEncoder.prepare(
                        "FLOW.COMPLETE",
                        List.of(
                                "flow-1",
                                "lease-1",
                                "FENCING",
                                3L,
                                "VALUE",
                                "result",
                                new byte[] {7},
                                "DROP_VALUE",
                                "scratch",
                                "OVERRIDE_VALUE",
                                "result"));

        assertEquals(0x020B, put.opcode());
        assertEquals("document", put.payload().get("name"));
        assertEquals(true, put.payload().get("override"));
        assertEquals(0x0204, complete.opcode());
        Map<?, ?> values = (Map<?, ?>) complete.payload().get("values");
        assertArrayEquals(new byte[] {7}, (byte[]) values.get("result"));
        assertEquals(List.of("scratch"), complete.payload().get("drop_values"));
        assertEquals(List.of("result"), complete.payload().get("override_values"));
    }
}
