package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FlowManyCommandEncoderTest {
    @Test
    void preparesMixedTransitionManyWithTheFullPublicOptionShape() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.TRANSITION_MANY",
                        List.of(
                                "MIXED",
                                "running",
                                "done",
                                "PAYLOAD",
                                new byte[] {1, 2},
                                "RUN_AT",
                                150L,
                                "PRIORITY",
                                4L,
                                "NOW",
                                100L,
                                "INDEPENDENT",
                                "true",
                                "RETURN",
                                "OK_ON_SUCCESS",
                                "ITEMS",
                                "a",
                                "p1",
                                1L,
                                "lease-a",
                                "b",
                                "p2",
                                2L,
                                "lease-b"));

        assertEquals(0x0211, prepared.opcode());
        assertEquals("running", prepared.payload().get("from_state"));
        assertEquals("done", prepared.payload().get("to_state"));
        assertEquals(100L, prepared.payload().get("now_ms"));
        assertEquals(150L, prepared.payload().get("run_at_ms"));
        assertEquals(4L, prepared.payload().get("priority"));
        assertEquals(true, prepared.payload().get("independent"));
        assertEquals("ok_on_success", prepared.payload().get("return"));
        assertEquals(
                List.of(
                        Map.of(
                                "id", "a",
                                "partition_key", "p1",
                                "fencing_token", 1L,
                                "lease_token", "lease-a"),
                        Map.of(
                                "id", "b",
                                "partition_key", "p2",
                                "fencing_token", 2L,
                                "lease_token", "lease-b")),
                prepared.payload().get("items"));
    }

    @Test
    void preparesFixedPartitionCompleteManyAndMutationFields() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.COMPLETE_MANY",
                        List.of(
                                "tenant-a",
                                "RESULT",
                                new byte[] {3},
                                "NOW",
                                100L,
                                "INDEPENDENT",
                                true,
                                "ATTRIBUTE_MERGE",
                                "attempt",
                                2L,
                                "DROP_VALUE",
                                "scratch",
                                "VALUE_REF",
                                "output",
                                "ref-1",
                                "RETURN",
                                "OK_ON_SUCCESS",
                                "ITEMS",
                                "a",
                                "lease-a",
                                1L));

        assertEquals(0x0210, prepared.opcode());
        assertEquals("tenant-a", prepared.payload().get("partition_key"));
        assertEquals(Map.of("attempt", 2L), prepared.payload().get("attributes_merge"));
        assertEquals(List.of("scratch"), prepared.payload().get("drop_values"));
        assertEquals(Map.of("output", "ref-1"), prepared.payload().get("value_refs"));
        assertEquals(List.of(List.of("a", "lease-a", 1L)), prepared.payload().get("items"));
    }

    @Test
    void fallsBackWithoutChangingMalformedOrUnsupportedCommands() {
        assertNull(FlowManyCommandEncoder.tryPrepare("FLOW.GET", List.of("id")));
        assertNull(
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.TRANSITION_MANY",
                        List.of("MIXED", "running", "done", "UNKNOWN", 1, "ITEMS")));
        assertNull(
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.COMPLETE_MANY",
                        List.of("MIXED", "NOW", 100L, "ITEMS", "a", "p1", "lease")));
        assertNull(
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.COMPLETE_MANY",
                        List.of(
                                "MIXED",
                                "RETURN",
                                new byte[] {(byte) 0xc3, 0x28},
                                "ITEMS",
                                "a",
                                "p1",
                                "lease-a",
                                1L)));
    }
}
