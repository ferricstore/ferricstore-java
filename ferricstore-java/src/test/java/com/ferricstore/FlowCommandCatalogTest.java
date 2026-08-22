package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class FlowCommandCatalogTest {
    private static final Set<String> OSS_0_11_9_FLOW_COMMANDS =
            Set.of(
                    "FLOW.CREATE",
                    "FLOW.GET",
                    "FLOW.CLAIM_DUE",
                    "FLOW.COMPLETE",
                    "FLOW.TRANSITION",
                    "FLOW.RETRY",
                    "FLOW.FAIL",
                    "FLOW.CANCEL",
                    "FLOW.EXTEND_LEASE",
                    "FLOW.HISTORY",
                    "FLOW.VALUE.PUT",
                    "FLOW.VALUE.MGET",
                    "FLOW.SIGNAL",
                    "FLOW.CREATE_MANY",
                    "FLOW.COMPLETE_MANY",
                    "FLOW.TRANSITION_MANY",
                    "FLOW.RETRY_MANY",
                    "FLOW.FAIL_MANY",
                    "FLOW.CANCEL_MANY",
                    "FLOW.RECLAIM",
                    "FLOW.REWIND",
                    "FLOW.INFO",
                    "FLOW.POLICY.SET",
                    "FLOW.POLICY.GET",
                    "FLOW.SPAWN_CHILDREN",
                    "FLOW.RETENTION_CLEANUP",
                    "FLOW.STEP_CONTINUE",
                    "FLOW.START_AND_CLAIM",
                    "FLOW.RUN_STEPS_MANY",
                    "FLOW.SCHEDULE.CREATE",
                    "FLOW.SCHEDULE.GET",
                    "FLOW.SCHEDULE.DELETE",
                    "FLOW.SCHEDULE.FIRE_DUE",
                    "FLOW.SCHEDULE.LIST",
                    "FLOW.SCHEDULE.FIRE",
                    "FLOW.SCHEDULE.PAUSE",
                    "FLOW.SCHEDULE.RESUME",
                    "FLOW.STATS",
                    "FLOW.ATTRIBUTES",
                    "FLOW.ATTRIBUTE_VALUES",
                    "FLOW.QUERY",
                    "FLOW.QUERY.INDEXES",
                    "FLOW.EFFECT.RESERVE",
                    "FLOW.EFFECT.CONFIRM",
                    "FLOW.EFFECT.FAIL",
                    "FLOW.EFFECT.COMPENSATE",
                    "FLOW.EFFECT.GET",
                    "FLOW.GOVERNANCE.LEDGER",
                    "FLOW.GOVERNANCE.OVERVIEW",
                    "FLOW.APPROVAL.REQUEST",
                    "FLOW.APPROVAL.APPROVE",
                    "FLOW.APPROVAL.REJECT",
                    "FLOW.APPROVAL.GET",
                    "FLOW.APPROVAL.LIST",
                    "FLOW.CIRCUIT.OPEN",
                    "FLOW.CIRCUIT.CLOSE",
                    "FLOW.CIRCUIT.GET",
                    "FLOW.BUDGET.RESERVE",
                    "FLOW.BUDGET.COMMIT",
                    "FLOW.BUDGET.RELEASE",
                    "FLOW.BUDGET.GET",
                    "FLOW.BUDGET.LIST",
                    "FLOW.LIMIT.LEASE",
                    "FLOW.LIMIT.SPEND",
                    "FLOW.LIMIT.RELEASE",
                    "FLOW.LIMIT.GET",
                    "FLOW.LIMIT.LIST");

    @Test
    void catalogExactlyMatchesTheOss0119FlowSurface() {
        Set<String> actual =
                Arrays.stream(FlowCommand.values())
                        .map(FlowCommand::wireName)
                        .collect(Collectors.toUnmodifiableSet());

        assertEquals(67, actual.size());
        assertEquals(OSS_0_11_9_FLOW_COMMANDS, actual);
        assertEquals(0x0231, FlowCommand.QUERY.nativeOpcode().orElseThrow());
        assertTrue(FlowCommand.QUERY_INDEXES.nativeOpcode().isEmpty());
    }
}
