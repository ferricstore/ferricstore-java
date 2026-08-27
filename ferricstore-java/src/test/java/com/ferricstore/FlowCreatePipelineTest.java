package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class FlowCreatePipelineTest {
    @Test
    void reusesEquivalentStringAndBinaryMetadataAcrossTheBatch() {
        String type = "order-🚀";
        String state = "quéued";
        FlowCreatePipeline.Batch batch =
                FlowCreatePipeline.tryParse(
                        List.of(
                                create("one", type, state),
                                create(
                                        "two",
                                        type.getBytes(StandardCharsets.UTF_8),
                                        state.getBytes(StandardCharsets.UTF_8))));

        assertEquals(2, batch.count());
        assertArrayEquals(type.getBytes(StandardCharsets.UTF_8), batch.metadata().type());
        assertArrayEquals(state.getBytes(StandardCharsets.UTF_8), batch.metadata().state());
    }

    @Test
    void rejectsDifferentOrMalformedRepeatedMetadata() {
        assertNull(
                FlowCreatePipeline.tryParse(
                        List.of(
                                create("one", "type", "queued"),
                                create("two", "other", "queued"))));
        assertNull(
                FlowCreatePipeline.tryParse(
                        List.of(
                                create("one", "type", "queued"),
                                create("two", "type", "queued\uD800"))));
    }

    private static List<Object> create(String id, Object type, Object state) {
        return List.of(
                "FLOW.CREATE", id, "TYPE", type, "STATE", state, "NOW", 1_000L, "RUN_AT", 1_000L);
    }
}
