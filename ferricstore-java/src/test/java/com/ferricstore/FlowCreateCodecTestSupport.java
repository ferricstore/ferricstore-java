package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class FlowCreateCodecTestSupport {
    private FlowCreateCodecTestSupport() {}

    static Fixture fixture() {
        byte[] binaryId = {0, (byte) 0xff};
        byte[] emptyPayload = {};
        FlowCreatePipeline.Batch batch =
                FlowCreatePipeline.tryParse(
                        List.of(create(binaryId, null), create("id-b", emptyPayload)));
        return new Fixture(batch, binaryId, emptyPayload);
    }

    static void assertPayload(Map<?, ?> payload, Fixture fixture) {
        assertArrayEquals(bytes("type"), assertInstanceOf(byte[].class, payload.get("type")));
        assertArrayEquals(bytes("queued"), assertInstanceOf(byte[].class, payload.get("state")));
        assertEquals(123L, assertInstanceOf(Number.class, payload.get("now_ms")).longValue());
        assertEquals(123L, assertInstanceOf(Number.class, payload.get("run_at_ms")).longValue());
        assertEquals(0L, assertInstanceOf(Number.class, payload.get("priority")).longValue());
        assertEquals(Boolean.TRUE, payload.get("independent"));
        assertEquals("ok_on_success", payload.get("return"));

        List<?> items = assertInstanceOf(List.class, payload.get("items"));
        assertEquals(2, items.size());
        Map<?, ?> first = assertInstanceOf(Map.class, items.get(0));
        Map<?, ?> second = assertInstanceOf(Map.class, items.get(1));
        assertArrayEquals(fixture.binaryId(), assertInstanceOf(byte[].class, first.get("id")));
        assertFalse(first.containsKey("payload"));
        assertArrayEquals(bytes("id-b"), assertInstanceOf(byte[].class, second.get("id")));
        assertTrue(second.containsKey("payload"));
        assertArrayEquals(
                fixture.emptyPayload(), assertInstanceOf(byte[].class, second.get("payload")));
    }

    private static List<Object> create(Object id, byte[] payload) {
        List<Object> command =
                new ArrayList<>(
                        List.of("FLOW.CREATE", id, "TYPE", "type", "STATE", "queued", "NOW", 123L));
        if (payload != null) {
            command.add("PAYLOAD");
            command.add(payload);
        }
        command.add("RUN_AT");
        command.add(123L);
        command.add("PRIORITY");
        command.add(0);
        return command;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    record Fixture(FlowCreatePipeline.Batch batch, byte[] binaryId, byte[] emptyPayload) {}
}
