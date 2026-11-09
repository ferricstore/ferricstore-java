package com.ferricstore;

import static com.ferricstore.CommandTestSupport.bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

final class WorkflowChildrenTest {
    private static final List<ChildSpec> CHILDREN =
            List.of(
                    new ChildSpec("report-1:part-1", "report-part", "1"),
                    new ChildSpec("report-1:part-2", "report-part", "2"));

    @Test
    void waitForChildrenSpawnsWithTheClaimInOneCommand() {
        ScriptedExecutor commands = new ScriptedExecutor(List.of(claimed()), "OK");
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "report",
                                "split")
                        .state(
                                "split",
                                context ->
                                        WaitForChildrenOutcome.builder(
                                                        CHILDREN,
                                                        "waiting_for_parts",
                                                        "join",
                                                        "failed")
                                                .groupId("parts")
                                                .onChildFailed("ignore")
                                                .onParentClosed("abandon_children")
                                                .build());

        assertEquals(1, workflow.worker("worker-a", List.of("split")).batchSize(1).runOnce());

        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.SPAWN_CHILDREN"), commands.names());
        assertSpawnedWithClaim(commands.last(), "parts");
        assertPair(commands.last(), "ON_CHILD_FAILED", "ignore");
        assertPair(commands.last(), "ON_PARENT_CLOSED", "abandon_children");
    }

    @Test
    void asyncHandlersCanReturnWaitForChildren() {
        ScriptedExecutor commands = new ScriptedExecutor(List.of(claimed()), "OK");
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "report",
                                "split")
                        .stateAsync(
                                "split",
                                context ->
                                        CompletableFuture.<Outcome>completedFuture(
                                                Outcomes.waitForChildren(
                                                        CHILDREN,
                                                        "waiting_for_parts",
                                                        "join",
                                                        "failed")));

        assertEquals(1, workflow.worker("worker-a", List.of("split")).batchSize(1).runOnce());

        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.SPAWN_CHILDREN"), commands.names());
        assertSpawnedWithClaim(commands.last(), "default");
    }

    @Test
    void waitForChildrenRejectsIncompleteOptions() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Outcomes.waitForChildren(List.of(), "waiting", "join", "failed"));
        assertThrows(
                IllegalArgumentException.class,
                () -> Outcomes.waitForChildren(CHILDREN, " ", "join", "failed"));
        assertThrows(
                IllegalArgumentException.class,
                () -> Outcomes.waitForChildren(CHILDREN, "waiting", null, "failed"));
        assertThrows(
                IllegalArgumentException.class,
                () -> Outcomes.waitForChildren(CHILDREN, "waiting", "join", ""));
        IllegalArgumentException none =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                WaitForChildrenOutcome.builder(
                                                CHILDREN, "waiting", "join", "failed")
                                        .waitMode("none")
                                        .build());
        assertTrue(none.getMessage().contains("all or any"), none.getMessage());
    }

    @Test
    void childResultsReadsTheGroupAndEveryValueInOneRead() {
        ScriptedExecutor commands =
                new ScriptedExecutor(parentWithGroup(), List.of(bytes("rows=3"), bytes("timeout")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        ChildGroupResults group = client.childResults("report-1", "tenant-1", "parts");

        assertEquals(
                List.of("FLOW.GET", "report-1", "PARTITION", "tenant-1"), commands.calls.get(0));
        assertEquals(
                List.of("FLOW.VALUE.MGET", "ref-result-1", "ref-error-2"), commands.calls.get(1));
        assertEquals(
                new ChildGroupResults(
                        "parts",
                        "all",
                        "success",
                        new ChildGroupSummary(3, 1, 1, 0),
                        List.of(
                                new ChildResult(
                                        "report-1:part-1", "tenant-1", "completed", "rows=3", null),
                                new ChildResult("report-1:part-2", null, "failed", null, "timeout"),
                                new ChildResult("report-1:part-3", null, "running", null, null))),
                group);
    }

    @Test
    void childResultsReturnsNullForAMissingGroupAndRejectsUnknownStatuses() {
        FerricStoreClient missing =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(parentWithGroup()), new StringCodec());
        assertNull(missing.childResults("report-1", null, "other"));

        Map<String, Object> parent = parentWithGroup();
        group(parent).put("children", Map.of("report-1:part-1", "paused"));
        FerricStoreClient unknown =
                FerricStoreClient.fromExecutor(new ScriptedExecutor(parent), new StringCodec());
        FerricStoreException failure =
                assertThrows(
                        FerricStoreException.class,
                        () -> unknown.childResults("report-1", null, "parts"));
        assertTrue(failure.getMessage().contains("paused"), failure.getMessage());
    }

    @Test
    void contextChildResultsReadsTheCurrentFlowAndRequiresTheGroup() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        List.of(claimed()),
                        parentWithGroup(),
                        List.of(bytes("1"), bytes("2")),
                        parentWithGroup(),
                        "OK");
        List<ChildGroupResults> seen = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "report",
                                "split")
                        .state(
                                "split",
                                context -> {
                                    seen.add(context.childResults("parts"));
                                    try {
                                        context.childResults("missing");
                                    } catch (FerricStoreException failure) {
                                        errors.add(failure.getMessage());
                                    }
                                    return Outcomes.complete("done");
                                });

        assertEquals(1, workflow.worker("worker-a", List.of("split")).batchSize(1).runOnce());

        assertEquals(3, seen.get(0).children().size());
        assertPair(commands.calls.get(1), "PARTITION", "tenant-1");
        assertEquals(List.of("Flow report-1 has no child group missing"), errors);
    }

    private static void assertSpawnedWithClaim(List<Object> command, String groupId) {
        assertEquals(
                List.of("FLOW.SPAWN_CHILDREN", "report-1", "GROUP", groupId, "WAIT", "all"),
                command.subList(0, 6));
        assertPair(command, "PARTITION", "tenant-1");
        assertPair(command, "LEASE_TOKEN", "lease-1");
        assertPair(command, "FENCING", 7L);
        assertPair(command, "WAIT_STATE", "waiting_for_parts");
        assertPair(command, "SUCCESS", "join");
        assertPair(command, "FAILURE", "failed");
        assertTrue(command.contains("report-1:part-1") && command.contains("report-1:part-2"));
    }

    private static void assertPair(List<Object> command, String name, Object value) {
        for (int index = 0; index + 1 < command.size(); index++) {
            if (name.equals(command.get(index)) && value.equals(command.get(index + 1))) {
                return;
            }
        }
        throw new AssertionError(command + " does not contain " + name + " " + value);
    }

    private static Map<String, Object> claimed() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", "report-1");
        response.put("type", "report");
        response.put("state", "running");
        response.put("run_state", "split");
        response.put("partition_key", "tenant-1");
        response.put("lease_token", "lease-1");
        response.put("fencing_token", 7L);
        response.put("version", 3L);
        return response;
    }

    private static Map<String, Object> parentWithGroup() {
        Map<String, Object> group = new LinkedHashMap<>();
        group.put("wait", "all");
        group.put("resolved", "success");
        group.put(
                "children",
                Map.of(
                        "report-1:part-2", "failed",
                        "report-1:part-1", "completed",
                        "report-1:part-3", "running"));
        group.put("child_partitions", Map.of("report-1:part-1", "tenant-1"));
        group.put(
                "results",
                Map.of(
                        "report-1:part-1", Map.of("result_ref", "ref-result-1"),
                        "report-1:part-2", Map.of("error_ref", "ref-error-2")));
        group.put("summary", Map.of("total", 3L, "completed", 1L, "failed", 1L, "cancelled", 0L));
        Map<String, Object> groups = new LinkedHashMap<>();
        groups.put("parts", group);
        Map<String, Object> parent = new LinkedHashMap<>();
        parent.put("id", "report-1");
        parent.put("type", "report");
        parent.put("state", "join");
        parent.put("partition_key", "tenant-1");
        parent.put("child_groups", groups);
        return parent;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> group(Map<String, Object> parent) {
        return (Map<String, Object>)
                ((Map<String, Object>) parent.get("child_groups")).get("parts");
    }

    private static final class ScriptedExecutor implements CommandExecutor {
        private final Deque<Object> responses = new ArrayDeque<>();
        private final List<List<Object>> calls = new ArrayList<>();

        private ScriptedExecutor(Object... responses) {
            for (Object response : responses) {
                this.responses.addLast(response);
            }
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if (responses.isEmpty()) {
                throw new AssertionError("unexpected command: " + args);
            }
            return responses.removeFirst();
        }

        private List<String> names() {
            return calls.stream().map(call -> String.valueOf(call.get(0))).toList();
        }

        private List<Object> last() {
            return calls.get(calls.size() - 1);
        }
    }
}
