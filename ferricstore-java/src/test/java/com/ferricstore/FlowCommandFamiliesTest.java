package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FlowCommandFamiliesTest {
    @Test
    void stepAndInsightFamiliesBuildCanonicalCommands() {
        CapturingExecutor executor = new CapturingExecutor(flowRecord(), "OK", Map.of(), List.of());
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.flowSteps()
                .continueStep(
                        FlowSteps.ContinueOptions.builder("run-1", "lease", 7, "running", "next")
                                .partitionKey("tenant")
                                .leaseMs(5_000)
                                .worker("worker")
                                .nowMs(100)
                                .value("input", "value")
                                .build());
        client.flowSteps()
                .runMany(
                        FlowSteps.RunManyOptions.builder(
                                        "order", List.of(new FlowSteps.RunItem("run-2", "tenant")))
                                .states(List.of("queued", "completed"))
                                .worker("worker")
                                .leaseMs(5_000)
                                .nowMs(100)
                                .build());
        client.flowInsights()
                .stats(
                        "order",
                        FlowInsights.StatsOptions.builder()
                                .state("queued")
                                .partitionKey("tenant")
                                .count(10)
                                .attribute("region", "eu")
                                .consistentProjection(true)
                                .build());
        client.flowInsights()
                .attributes(
                        "order",
                        FlowInsights.ReadOptions.builder()
                                .partitionKey("tenant")
                                .count(10)
                                .build());

        assertEquals(
                List.of(
                        "FLOW.STEP_CONTINUE",
                        "run-1",
                        "lease",
                        "running",
                        "next",
                        "FENCING",
                        7L,
                        "LEASE_MS",
                        5_000L,
                        "NOW",
                        100L,
                        "PARTITION",
                        "tenant",
                        "WORKER",
                        "worker",
                        "VALUE",
                        "input"),
                executor.calls.get(0).subList(0, executor.calls.get(0).size() - 1));
        assertArrayEquals(
                bytes("value"),
                (byte[]) executor.calls.get(0).get(executor.calls.get(0).size() - 1));
        assertEquals("FLOW.RUN_STEPS_MANY", executor.calls.get(1).get(0));
        assertOption(executor.calls.get(1), "STATES", List.of("queued", "completed"));
        assertOption(
                executor.calls.get(1),
                "ITEMS",
                List.of(Map.of("id", "run-2", "partition_key", "tenant")));
        assertEquals("FLOW.STATS", executor.calls.get(2).get(0));
        assertOption(executor.calls.get(2), "ATTRIBUTE", "region");
        assertEquals(
                "eu", executor.calls.get(2).get(executor.calls.get(2).indexOf("ATTRIBUTE") + 2));
        assertOption(executor.calls.get(2), "CONSISTENT_PROJECTION", "true");
        assertEquals("FLOW.ATTRIBUTES", executor.calls.get(3).get(0));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlowSteps.RunManyOptions.builder(
                                        "order", List.of(new FlowSteps.RunItem("run", null)))
                                .worker("worker")
                                .build());
    }

    @Test
    void scheduleFamilyCoversEveryScheduleCommand() {
        CapturingExecutor executor =
                new CapturingExecutor(
                        Map.of(), Map.of(), Map.of(), Map.of(), "OK", Map.of(), Map.of(),
                        List.of());
        FlowSchedules schedules = FerricStoreClient.fromExecutor(executor).flowSchedules();

        schedules.create(
                "daily",
                FlowSchedules.CreateOptions.builder(Map.of("type", "report"))
                        .cron("0 0 * * *")
                        .timezone("UTC")
                        .overwrite(true)
                        .nowMs(100)
                        .build());
        schedules.get("daily");
        schedules.fire("daily", 200L, 100L);
        schedules.pause("daily", 100L);
        schedules.delete("daily", 100L);
        schedules.resume("daily", 100L);
        schedules.fireDue(
                FlowSchedules.FireDueOptions.builder()
                        .worker("scheduler")
                        .leaseMs(30_000)
                        .limit(25)
                        .build());
        schedules.list(
                FlowSchedules.ListOptions.builder()
                        .kind("cron")
                        .state("active")
                        .count(25)
                        .reverse(true)
                        .build());

        assertNames(
                executor,
                "FLOW.SCHEDULE.CREATE",
                "FLOW.SCHEDULE.GET",
                "FLOW.SCHEDULE.FIRE",
                "FLOW.SCHEDULE.PAUSE",
                "FLOW.SCHEDULE.DELETE",
                "FLOW.SCHEDULE.RESUME",
                "FLOW.SCHEDULE.FIRE_DUE",
                "FLOW.SCHEDULE.LIST");
        assertOption(executor.calls.get(0), "TARGET", Map.of("type", "report"));
        assertOption(executor.calls.get(0), "OVERWRITE", "true");
    }

    @Test
    void governanceFamilyCoversEveryGovernanceCommand() {
        CapturingExecutor executor = new CapturingExecutor();
        FlowGovernance governance = FerricStoreClient.fromExecutor(executor).flowGovernance();

        governance.ledger(
                "run-1",
                FlowGovernance.LedgerOptions.builder().partitionKey("tenant").limit(20).build());
        governance.approvalRequest(
                "approval-1",
                FlowGovernance.ApprovalRequestOptions.builder("run-1", "tenant:payments")
                        .requestedBy("service")
                        .assignees(List.of("alice", "bob"))
                        .timeoutMs(30_000)
                        .build());
        governance.approvalApprove("approval-1", "alice", "safe", 100L);
        governance.approvalReject("approval-2", "bob", "unsafe", 100L);
        governance.approvalGet("approval-1");
        governance.approvalList(
                FlowGovernance.ApprovalListOptions.builder().status("pending").limit(20).build());
        governance.overview(
                FlowGovernance.ApprovalListOptions.builder().scope("tenant:payments").build());
        governance.circuitOpen(
                "tenant:payments",
                FlowGovernance.CircuitOpenOptions.builder()
                        .openMs(30_000)
                        .failureThreshold(5)
                        .errorClasses(List.of("timeout"))
                        .build());
        governance.circuitClose("tenant:payments", 100L);
        governance.circuitGet("tenant:payments");
        governance.budgetReserve(
                "tenant:tokens",
                100,
                FlowGovernance.BudgetReserveOptions.builder()
                        .limit(1_000)
                        .windowMs(60_000)
                        .reservationId("reservation-1")
                        .build());
        governance.budgetCommit("tenant:tokens", "reservation-1", 80, Map.of("tokens", 80), 100L);
        governance.budgetRelease("tenant:tokens", "reservation-2", 100L);
        governance.budgetGet("tenant:tokens");
        governance.budgetList(
                FlowGovernance.FilterOptions.builder().scope("tenant:tokens").limit(20).build());
        governance.limitLease("tenant:rps", 1, 20, 30_000, 1_000L, 100L);
        governance.limitSpend("tenant:rps", 1, 10, 100L);
        governance.limitRelease("tenant:rps", 1, List.of("reservation-1"), 100L);
        governance.limitGet("tenant:rps", 100L);
        governance.limitList(
                FlowGovernance.FilterOptions.builder().partitionKey("tenant").limit(20).build());

        assertNames(
                executor,
                "FLOW.GOVERNANCE.LEDGER",
                "FLOW.APPROVAL.REQUEST",
                "FLOW.APPROVAL.APPROVE",
                "FLOW.APPROVAL.REJECT",
                "FLOW.APPROVAL.GET",
                "FLOW.APPROVAL.LIST",
                "FLOW.GOVERNANCE.OVERVIEW",
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
        assertOption(executor.calls.get(1), "ASSIGNEES", List.of("alice", "bob"));
        assertOption(executor.calls.get(7), "ERROR_CLASSES", List.of("timeout"));
        assertOption(executor.calls.get(11), "USAGE", Map.of("tokens", 80));
        assertOption(executor.calls.get(17), "RESERVATION_IDS", List.of("reservation-1"));
    }

    private static Map<String, Object> flowRecord() {
        return Map.of(
                "id", "run-1",
                "type", "order",
                "state", "next",
                "version", 2L,
                "fencing_token", 8L);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertNames(CapturingExecutor executor, String... names) {
        assertEquals(List.of(names), executor.calls.stream().map(call -> call.get(0)).toList());
    }

    private static void assertOption(List<Object> command, String name, Object expected) {
        int index = command.indexOf(name);
        assertEquals(expected, command.get(index + 1));
    }

    private static final class CapturingExecutor implements CommandExecutor {
        private final List<Object> responses = new ArrayList<>();
        private final List<List<Object>> calls = new ArrayList<>();

        private CapturingExecutor(Object... responses) {
            this.responses.addAll(List.of(responses));
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(new ArrayList<>(args));
            if (responses.isEmpty()) {
                String name = String.valueOf(args.get(0));
                if ("FLOW.GOVERNANCE.LEDGER".equals(name)
                        || "FLOW.APPROVAL.LIST".equals(name)
                        || "FLOW.BUDGET.LIST".equals(name)
                        || "FLOW.LIMIT.LIST".equals(name)) {
                    return List.of();
                }
                return new LinkedHashMap<>();
            }
            return responses.remove(0);
        }
    }
}
