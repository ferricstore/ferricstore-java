package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FlowItemCommandEncoderTest {
    @Test
    void preparesCreateManyItemMapsWithoutLeakingSdkMarkers() {
        FlowItemCommandEncoder.Prepared prepared =
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.CREATE_MANY",
                        List.of(
                                "AUTO",
                                "TYPE",
                                "order",
                                "STATE",
                                "queued",
                                "NOW",
                                100L,
                                "ITEMS_MAPS",
                                1,
                                Map.of(
                                        "id",
                                        "flow-1",
                                        "payload",
                                        new byte[] {1},
                                        "values",
                                        Map.of("document", new byte[] {2}),
                                        "max_active_ms",
                                        5_000L)));

        assertEquals(0x020F, prepared.opcode());
        assertEquals("order", prepared.payload().get("type"));
        assertEquals(1, ((List<?>) prepared.payload().get("items")).size());
    }

    @Test
    void preparesExtendedMixedCreateAndMappedChildren() {
        FlowItemCommandEncoder.Prepared create =
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.CREATE_MANY",
                        List.of(
                                "MIXED",
                                "TYPE",
                                "order",
                                "ITEMS_EXT",
                                1,
                                "flow-1",
                                "tenant-a",
                                new byte[] {1},
                                1,
                                "document",
                                new byte[] {2},
                                1,
                                "shared",
                                "ref-1"));
        FlowItemCommandEncoder.Prepared spawn =
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.SPAWN_CHILDREN",
                        List.of(
                                "parent-1",
                                "GROUP",
                                "group-1",
                                "FENCING",
                                2L,
                                "ITEMS_MAPS",
                                1,
                                Map.of(
                                        "id",
                                        "child-1",
                                        "type",
                                        "child",
                                        "payload",
                                        new byte[] {3},
                                        "max_active_ms",
                                        10_000L)));

        Map<?, ?> createItem = (Map<?, ?>) ((List<?>) create.payload().get("items")).get(0);
        assertEquals("tenant-a", createItem.get("partition_key"));
        assertEquals(Map.of("shared", "ref-1"), createItem.get("value_refs"));
        assertEquals(0x0220, spawn.opcode());
        assertEquals("parent-1", spawn.payload().get("id"));
    }

    @Test
    void rejectsUnsupportedAndMalformedShapes() {
        assertNull(FlowItemCommandEncoder.tryPrepare("FLOW.GET", List.of("flow-1")));
        assertNull(
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.CREATE_MANY", List.of("AUTO", "ITEMS_MAPS", 2, Map.of())));
    }

    @Test
    void optionValuesThatEqualItemMarkersRemainUserData() {
        FlowItemCommandEncoder.Prepared prepared =
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.CREATE_MANY",
                        List.of(
                                "AUTO",
                                "TYPE",
                                "order",
                                "STATE",
                                "ITEMS",
                                "ATTRIBUTE",
                                "marker",
                                "ITEMS_MAPS",
                                "ITEMS",
                                "flow-1",
                                new byte[] {1}));

        assertEquals("ITEMS", prepared.payload().get("state"));
        assertEquals(Map.of("marker", "ITEMS_MAPS"), prepared.payload().get("attributes"));
        assertEquals(1, ((List<?>) prepared.payload().get("items")).size());
    }

    @Test
    void childIdThatEqualsFormerMixedMarkerRemainsUserData() {
        FlowItemCommandEncoder.Prepared prepared =
                FlowItemCommandEncoder.tryPrepare(
                        "FLOW.SPAWN_CHILDREN",
                        List.of(
                                "parent-1",
                                "PARTITION",
                                "tenant-a",
                                "ITEMS",
                                "MIXED",
                                "child-type",
                                new byte[] {1}));

        Map<?, ?> child = (Map<?, ?>) ((List<?>) prepared.payload().get("children")).get(0);
        assertEquals("MIXED", child.get("id"));
        assertEquals("child-type", child.get("type"));
    }
}
