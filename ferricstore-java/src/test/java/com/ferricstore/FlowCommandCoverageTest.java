package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class FlowCommandCoverageTest {
    @Test
    void flowLifecycleCoversRichSingleAndBulkCommandShapes() {
        FlowRespondingExecutor executor = new FlowRespondingExecutor();
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new StringCodec());
        FlowMutationFields mutations =
                FlowMutationFields.builder()
                        .attributeMerge("owner", "team")
                        .attributesMerge(Map.of("region", "eu"))
                        .attributeDelete("old")
                        .attributesDelete(List.of("legacy"))
                        .stateMeta("running", Map.of("color", "blue"))
                        .stateMeta(Map.of("queued", Map.of("rank", 1)))
                        .dropValue("drop")
                        .dropValues(List.of("drop-too"))
                        .overrideValue("replace")
                        .overrideValues(List.of("replace-too"))
                        .build();
        FlowMutationFields retryMutations =
                FlowMutationFields.builder()
                        .attributeMerge("owner", "team")
                        .attributesMerge(Map.of("region", "eu"))
                        .attributeDelete("old")
                        .attributesDelete(List.of("legacy"))
                        .stateMeta("running", Map.of("color", "blue"))
                        .stateMeta(Map.of("queued", Map.of("rank", 1)))
                        .build();

        CreateOptions create =
                CreateOptions.builder("flow-1", "order")
                        .state("queued")
                        .payload("payload")
                        .partitionKey("tenant")
                        .parentFlowId("parent")
                        .rootFlowId("root")
                        .correlationId("correlation")
                        .runAtMs(120)
                        .nowMs(100)
                        .priority(5)
                        .idempotent(true)
                        .retentionTtlMs(1_000)
                        .maxActiveMs(MaxActiveMs.infinity())
                        .attribute("owner", "team")
                        .attributes(Map.of("region", "eu"))
                        .stateMeta("queued", Map.of("rank", 1))
                        .stateMeta(Map.of("running", Map.of("rank", 2)))
                        .value("input", "value")
                        .values(Map.of("other", "value"))
                        .valueRef("shared", "ref-1")
                        .valueRefs(Map.of("second", "ref-2"))
                        .returnRecord(true)
                        .build();
        assertNotNull(client.create(create));
        assertNotNull(client.enqueue("flow-2", create));

        CreateItem mappedItem =
                new CreateItem(
                        "flow-3",
                        "payload",
                        "tenant",
                        Map.of("input", "value"),
                        Map.of("shared", "ref-1"),
                        MaxActiveMs.of(1_000));
        client.createMany(
                CreateManyOptions.builder("order", List.of(mappedItem))
                        .partitionKey("tenant")
                        .state("queued")
                        .runAtMs(120)
                        .nowMs(100)
                        .priority(5)
                        .idempotent(true)
                        .independent(true)
                        .retentionTtlMs(1_000)
                        .maxActiveMs(2_000)
                        .attribute("owner", "team")
                        .attributes(Map.of("region", "eu"))
                        .stateMeta("queued", Map.of("rank", 1))
                        .stateMeta(Map.of("running", Map.of("rank", 2)))
                        .value("default", "value")
                        .values(Map.of("second", "value"))
                        .valueRef("shared", "ref-1")
                        .valueRefs(Map.of("other", "ref-2"))
                        .build());

        assertNull(
                client.startAndClaim(
                        StartAndClaimOptions.builder("flow-4", "order", "queued", "worker-1")
                                .leaseMs(5_000)
                                .payload("payload")
                                .partitionKey("tenant")
                                .parentFlowId("parent")
                                .rootFlowId("root")
                                .correlationId("correlation")
                                .nowMs(100)
                                .priority(5)
                                .retentionTtlMs(1_000)
                                .maxActiveMs(2_000)
                                .attribute("owner", "team")
                                .stateMeta("queued", Map.of("rank", 1))
                                .value("input", "value")
                                .valueRef("shared", "ref-1")
                                .build()));

        assertFalse(client.valuePut("value", "name", "flow-1", "tenant", 1_000L).isEmpty());
        assertFalse(client.valuePut("value", "name", "flow-1", "tenant", 1_000L, true).isEmpty());
        assertEquals(List.of(), client.valueMGet(List.of()));
        assertEquals(List.of("value"), client.valueMGet(List.of("ref-1"), 1_000L));

        client.signal(
                "flow-1",
                "approved",
                "running",
                "tenant",
                Map.of("approval", true),
                List.of("queued"));
        client.signal(
                "flow-1",
                SignalOptions.builder("approved")
                        .partitionKey("tenant")
                        .idempotencyKey("signal-1")
                        .ifState("queued")
                        .ifStates(List.of("waiting"))
                        .transitionTo("running")
                        .runAtMs(120)
                        .nowMs(100)
                        .value("approval", true)
                        .valueRef("shared", "ref-1")
                        .dropValue("old")
                        .overrideValue("approval")
                        .build());

        ClaimDueOptions claim =
                ClaimDueOptions.builder("order", "worker-1")
                        .states(List.of("queued", "retrying"))
                        .partitionKeys(List.of("tenant", "tenant-2"))
                        .leaseMs(5_000)
                        .limit(10)
                        .nowMs(100)
                        .blockMs(50)
                        .priority(5)
                        .reclaimExpired(true)
                        .reclaimRatio(2)
                        .payload(true)
                        .payloadMaxBytes(1_024)
                        .value("input")
                        .values(List.of("other"))
                        .valueMaxBytes(1_024)
                        .jobOnly(true)
                        .includeState(true)
                        .includeAttributes(true)
                        .build();
        assertEquals(List.of(), client.claimDue(claim));
        assertEquals(List.of(), client.claimJobs(claim));
        ClaimDueOptions reclaim =
                ClaimDueOptions.builder("order", "worker-1")
                        .partitionKeys(List.of("tenant", "tenant-2"))
                        .leaseMs(5_000)
                        .limit(10)
                        .nowMs(100)
                        .priority(5)
                        .payload(true)
                        .payloadMaxBytes(1_024)
                        .values(List.of("input", "other"))
                        .valueMaxBytes(1_024)
                        .jobOnly(true)
                        .includeState(true)
                        .includeAttributes(true)
                        .build();
        assertEquals(List.of(), client.reclaim(reclaim));
        assertEquals(List.of(), client.reclaimJobs(reclaim));
        client.extendLease("flow-1", "lease", 1, 5_000, "tenant");

        assertNotNull(
                client.transition(
                        TransitionOptions.builder("flow-1", "queued", "running", "lease", 1)
                                .partitionKey("tenant")
                                .payload("payload")
                                .runAtMs(120)
                                .nowMs(100)
                                .priority(5)
                                .value("input", "value")
                                .valueRef("shared", "ref-1")
                                .mutationFields(mutations)
                                .returnRecord(true)
                                .build()));
        assertNotNull(
                client.complete(
                        CompleteOptions.builder("flow-1", "lease", 1)
                                .partitionKey("tenant")
                                .result("result")
                                .payload("payload")
                                .ttlMs(1_000)
                                .nowMs(100)
                                .value("output", "value")
                                .valueRef("shared", "ref-1")
                                .mutationFields(mutations)
                                .returnRecord(true)
                                .build()));
        assertNotNull(
                client.retry(
                        RetryOptions.builder("flow-1", "lease", 1)
                                .partitionKey("tenant")
                                .error("retry")
                                .payload("payload")
                                .runAtMs(120)
                                .nowMs(100)
                                .mutationFields(retryMutations)
                                .returnRecord(true)
                                .build()));
        assertNotNull(
                client.fail(
                        FailOptions.builder("flow-1", "lease", 1)
                                .partitionKey("tenant")
                                .error("failed")
                                .payload("payload")
                                .ttlMs(1_000)
                                .nowMs(100)
                                .value("output", "value")
                                .valueRef("shared", "ref-1")
                                .mutationFields(mutations)
                                .returnRecord(true)
                                .build()));
        assertNotNull(
                client.cancel(
                        CancelOptions.builder("flow-1", 1)
                                .leaseToken("lease")
                                .partitionKey("tenant")
                                .reason("cancelled")
                                .ttlMs(1_000)
                                .nowMs(100)
                                .value("output", "value")
                                .valueRef("shared", "ref-1")
                                .mutationFields(mutations)
                                .returnRecord(true)
                                .build()));

        ClaimedItem claimed = new ClaimedItem("flow-1", "lease", 1, "tenant");
        FencedItem fenced = new FencedItem("flow-1", 1, "lease", "tenant");
        client.completeMany(
                CompleteManyOptions.builder(List.of(claimed))
                        .partitionKey("tenant")
                        .result("result")
                        .payload("payload")
                        .ttlMs(1_000)
                        .nowMs(100)
                        .independent(true)
                        .value("output", "value")
                        .valueRef("shared", "ref-1")
                        .mutationFields(mutations)
                        .returnOkOnSuccess(true)
                        .build());
        client.transitionMany(
                TransitionManyOptions.builder("queued", "running", List.of(fenced))
                        .partitionKey("tenant")
                        .payload("payload")
                        .runAtMs(120)
                        .nowMs(100)
                        .priority(5)
                        .independent(true)
                        .value("output", "value")
                        .valueRef("shared", "ref-1")
                        .mutationFields(mutations)
                        .returnOkOnSuccess(true)
                        .build());
        client.retryMany(
                RetryManyOptions.builder(List.of(claimed))
                        .partitionKey("tenant")
                        .error("retry")
                        .payload("payload")
                        .runAtMs(120)
                        .nowMs(100)
                        .independent(true)
                        .mutationFields(retryMutations)
                        .build());
        client.failMany(
                FailManyOptions.builder(List.of(claimed))
                        .partitionKey("tenant")
                        .error("failed")
                        .payload("payload")
                        .ttlMs(1_000)
                        .nowMs(100)
                        .independent(true)
                        .value("output", "value")
                        .valueRef("shared", "ref-1")
                        .mutationFields(mutations)
                        .build());
        client.cancelMany(
                CancelManyOptions.builder(List.of(fenced))
                        .partitionKey("tenant")
                        .reason("cancelled")
                        .ttlMs(1_000)
                        .nowMs(100)
                        .independent(true)
                        .value("output", "value")
                        .valueRef("shared", "ref-1")
                        .mutationFields(mutations)
                        .build());

        assertTrue(
                executor.commandNames()
                        .containsAll(
                                Set.of(
                                        "FLOW.CREATE_MANY",
                                        "FLOW.START_AND_CLAIM",
                                        "FLOW.TRANSITION_MANY",
                                        "FLOW.CANCEL_MANY")));
    }

    @Test
    @SuppressWarnings("PMD.NcssCount") // Keep the complete public command audit in one scenario.
    void flowInspectionPolicyEffectsAndAdministrationSurfacesAreComplete() {
        FlowRespondingExecutor executor = new FlowRespondingExecutor();
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new StringCodec());

        assertNull(client.get("flow-1", "tenant"));
        assertEquals(List.of(), client.list("order", "queued", "tenant", 10));
        client.rewind("flow-1", "event-1", "tenant", "running", 120L, "reason", 100L, false);
        assertEquals(List.of(), client.terminals("order", null, "tenant", 10));
        assertEquals(List.of(), client.failures("order", "tenant", 10));
        assertEquals(List.of(), client.byParent("parent", "tenant", 10));
        assertEquals(List.of(), client.byRoot("root", "tenant", 10));
        assertEquals(List.of(), client.byCorrelation("correlation", "tenant", 10));
        assertEquals(List.of(), client.stuck("order", "tenant", 10, 1_000L, 5_000L));
        client.history("flow-1", "tenant", 10);
        client.history(
                "flow-1",
                HistoryOptions.builder()
                        .partitionKey("tenant")
                        .count(10)
                        .fromEvent("event-1")
                        .toEvent("event-2")
                        .fromMs(1)
                        .toMs(2)
                        .fromVersion(1)
                        .toVersion(2)
                        .reverse(true)
                        .event("transition")
                        .worker("worker-1")
                        .includeCold(true)
                        .consistentProjection(true)
                        .values(true)
                        .payloadMaxBytes(1_024)
                        .build());
        assertFalse(client.flowInfo("order").isEmpty());

        ChildSpec child =
                new ChildSpec(
                        "child-1",
                        "child",
                        "payload",
                        "tenant",
                        Map.of("input", "value"),
                        Map.of("shared", "ref-1"),
                        MaxActiveMs.of(1_000));
        client.spawnChildren(
                SpawnChildrenOptions.builder("flow-1", List.of(child))
                        .partitionKey("tenant")
                        .leaseToken("lease")
                        .fencingToken(1)
                        .groupId("group-1")
                        .waitMode("all")
                        .waitState("waiting")
                        .success("completed")
                        .failure("failed")
                        .fromState("running")
                        .onChildFailed("fail")
                        .onParentClosed("cancel")
                        .maxActiveMs(MaxActiveMs.infinity())
                        .value("default", "value")
                        .values(Map.of("other", "value"))
                        .valueRef("shared", "ref-1")
                        .valueRefs(Map.of("second", "ref-2"))
                        .nowMs(100)
                        .build());

        RetryPolicy retry = new RetryPolicy(3, "exponential", 10L, 1_000L, 20, "failed");
        client.installPolicy(
                "order",
                FlowPolicyOptions.builder()
                        .state("queued")
                        .retry(retry)
                        .retentionTtlMs(1_000)
                        .maxActiveMs(MaxActiveMs.infinity())
                        .replace(true)
                        .expectedGeneration(2)
                        .indexedAttribute("owner")
                        .indexedAttributes(List.of("region"))
                        .indexedStateMeta("queued")
                        .mode(FlowStateMode.FIFO)
                        .statePolicy("running", FlowStatePolicy.parallel(retry))
                        .statePolicies(Map.of("retrying", FlowStatePolicy.fifo()))
                        .build());
        assertFalse(client.policyGet("order", "queued").isEmpty());

        EffectReserveOptions reserve =
                EffectReserveOptions.builder("lease", 1)
                        .partitionKey("tenant")
                        .operationDigest("digest")
                        .idempotencyKey("effect-1")
                        .governanceScope("tenant")
                        .nowMs(100)
                        .build();
        assertFalse(client.effectReserve("flow-1", "email", "send", reserve).isEmpty());
        EffectStatusOptions status =
                EffectStatusOptions.builder()
                        .partitionKey("tenant")
                        .lease("lease", 1)
                        .externalId("external-1")
                        .error("error")
                        .reason("reason")
                        .latencyMs(10)
                        .nowMs(100)
                        .build();
        assertFalse(client.effectConfirm("flow-1", "email", status).isEmpty());
        assertFalse(client.effectFail("flow-1", "email", status).isEmpty());
        assertFalse(client.effectCompensate("flow-1", "email", status).isEmpty());
        assertFalse(client.effectGet("flow-1", "email", "tenant").isEmpty());
        assertFalse(client.retentionCleanup(10, 100L).isEmpty());

        client.ping();
        client.ping("message");
        client.echo("message");
        client.dbsize();
        assertTrue(client.flushdb("ASYNC"));
        assertTrue(client.flushall("ASYNC"));
        client.commandInfo("GET", "SET");
        client.slowlog("GET", 10);
        client.memory("USAGE", "key");
        client.config("GET", "*");
        client.publish("channel", "message");
        client.pubsub("CHANNELS", "*");
        assertTrue(client.cas("key", "old", "new", 10L));
        assertTrue(client.lock("key", "owner", 1_000));
        client.unlock("key", "owner");
        client.extendLock("key", "owner", 1_000);
        assertTrue(client.ratelimitAdd("key", 1_000, 10, 1).allowed());
        assertFalse(client.keyInfo("key").isEmpty());
        assertTrue(client.fetchOrCompute("key", 1_000, null).hit());
        assertTrue(client.fetchOrCompute("key", 1_000, "hint").shouldCompute());
        assertTrue(client.fetchOrComputeResult("key", "token", "value", 1_000));
        assertTrue(client.fetchOrComputeError("key", "token", "message"));
        assertFalse(client.clusterHealth().isEmpty());
        assertFalse(client.clusterStats().isEmpty());
        client.clusterKeyslot("key");
        client.clusterSlots();
        assertFalse(client.clusterStatus().isEmpty());
        client.clusterRole();
        assertTrue(client.clusterJoin("node", false));
        assertTrue(client.clusterJoin("node", true));
        assertTrue(client.clusterLeave());
        assertTrue(client.clusterFailover(1, "node"));
        assertTrue(client.clusterPromote("node"));
        assertTrue(client.clusterDemote("node"));
        client.ferricstoreConfig("GET", "*");
        assertFalse(client.ferricstoreMetrics().isEmpty());
        assertFalse(client.ferricstoreHotness().isEmpty());
        client.ferricstoreBlobgc("RUN");
        client.ferricstoreDoctor("CHECK");
        assertEquals("server", client.serverInfo("server"));
        client.close();

        assertThrows(IllegalStateException.class, client::transaction);
        assertThrows(IllegalStateException.class, client::pubsubSession);
        assertTrue(
                executor.commandNames()
                        .containsAll(
                                Set.of(
                                        "FLOW.POLICY.SET",
                                        "FLOW.EFFECT.COMPENSATE",
                                        "FETCH_OR_COMPUTE_RESULT",
                                        "CLUSTER.FAILOVER")));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class FlowRespondingExecutor implements CommandExecutor {
        private final List<List<Object>> calls = new ArrayList<>();
        private boolean recordReadPending;

        @Override
        public Object execute(List<Object> command) {
            List<Object> copy = List.copyOf(command);
            calls.add(copy);
            String name = String.valueOf(copy.get(0));
            if (name.startsWith("FLOW.CLAIM")
                    || "FLOW.RECLAIM".equals(name)
                    || name.endsWith("_MANY")
                    || "FLOW.HISTORY".equals(name)) {
                return List.of();
            }
            if ("FLOW.GET".equals(name)) {
                if (!recordReadPending) {
                    return null;
                }
                recordReadPending = false;
                return Map.of(
                        "id",
                        copy.get(1),
                        "type",
                        "order",
                        "state",
                        "queued",
                        "fencing_token",
                        1L,
                        "version",
                        1L);
            }
            if ("FLOW.CREATE".equals(name)
                    || "FLOW.TRANSITION".equals(name)
                    || "FLOW.COMPLETE".equals(name)
                    || "FLOW.RETRY".equals(name)
                    || "FLOW.FAIL".equals(name)
                    || "FLOW.CANCEL".equals(name)) {
                recordReadPending = true;
                return null;
            }
            if ("FLOW.START_AND_CLAIM".equals(name)) {
                return null;
            }
            if ("FLOW.VALUE.MGET".equals(name)) {
                return List.of(bytes("value"));
            }
            if ("RATELIMIT.ADD".equals(name)) {
                return List.of("allowed", 1L, 9L, 1_000L);
            }
            if ("FETCH_OR_COMPUTE".equals(name)) {
                return copy.size() == 3
                        ? List.of("hit", bytes("value"))
                        : List.of("compute", "hint", "token");
            }
            if ("INFO".equals(name)) {
                return bytes("server");
            }
            if ("CAS".equals(name)) {
                return "1";
            }
            if (Set.of(
                            "FLUSHDB",
                            "FLUSHALL",
                            "LOCK",
                            "FETCH_OR_COMPUTE_RESULT",
                            "FETCH_OR_COMPUTE_ERROR",
                            "CLUSTER.JOIN",
                            "CLUSTER.LEAVE",
                            "CLUSTER.FAILOVER",
                            "CLUSTER.PROMOTE",
                            "CLUSTER.DEMOTE")
                    .contains(name)) {
                return "OK";
            }
            if (Set.of("DBSIZE", "PUBLISH", "UNLOCK", "EXTEND", "CLUSTER.KEYSLOT").contains(name)) {
                return 1L;
            }
            if (name.startsWith("FLOW.VALUE.PUT")
                    || name.startsWith("FLOW.EFFECT")
                    || "FLOW.INFO".equals(name)
                    || "FLOW.POLICY.GET".equals(name)
                    || "FLOW.RETENTION_CLEANUP".equals(name)
                    || name.startsWith("FERRICSTORE.KEY_INFO")
                    || name.startsWith("FERRICSTORE.METRICS")
                    || name.startsWith("FERRICSTORE.HOTNESS")
                    || name.startsWith("CLUSTER.HEALTH")
                    || name.startsWith("CLUSTER.STATS")
                    || name.startsWith("CLUSTER.STATUS")) {
                return Map.of("ok", 1L);
            }
            return "OK";
        }

        @Override
        public Object flowQuery(String query, Map<String, ?> params) {
            return Map.of(
                    "version",
                    "ferric.flow.query.result/v1",
                    "records",
                    List.of(),
                    "page",
                    Map.of("has_more", false),
                    "quality",
                    Map.of(),
                    "usage",
                    Map.of("result_records", 0L));
        }

        private Set<String> commandNames() {
            java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
            calls.forEach(call -> names.add(String.valueOf(call.get(0))));
            return names;
        }
    }
}
