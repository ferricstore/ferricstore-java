package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

final class NativeRoutingTest {
    @Test
    void routesIndependentClaimWorkersToIndependentNativeLanes() {
        assertEquals(
                "worker-7",
                NativeRouting.routeKey(
                        List.of(
                                "FLOW.CLAIM_DUE",
                                "orders",
                                "STATE",
                                "queued",
                                "WORKER",
                                "worker-7",
                                "LIMIT",
                                100)));
    }

    @Test
    void routesManyMutationsByTheirFirstFlowInsteadOfTheMixedSentinel() {
        assertEquals(
                "flow-a",
                NativeRouting.routeKey(
                        List.of(
                                "FLOW.TRANSITION_MANY",
                                "MIXED",
                                "running",
                                "done",
                                "NOW",
                                123L,
                                "ITEMS",
                                "flow-a",
                                "partition-a",
                                5L,
                                "lease-a")));
    }
}
