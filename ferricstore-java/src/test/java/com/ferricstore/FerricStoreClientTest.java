package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FerricStoreClientTest {
    @Test
    void createBuildsCommandDefaults() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.create(CreateOptions.builder("flow-1", "order")
            .state("created")
            .partitionKey("tenant-a")
            .payload(bytes("payload"))
            .parentFlowId("parent")
            .rootFlowId("root")
            .correlationId("corr")
            .nowMs(100)
            .runAtMs(120)
            .priority(5)
            .idempotent(true)
            .build());

        assertArgs(List.of(
            "FLOW.CREATE", "flow-1", "TYPE", "order", "STATE", "created", "NOW", 100L,
            "PARTITION", "tenant-a", "PAYLOAD", bytes("payload"),
            "PARENT_FLOW_ID", "parent", "ROOT_FLOW_ID", "root", "CORRELATION_ID", "corr",
            "RUN_AT", 120L, "PRIORITY", 5L, "IDEMPOTENT", "true"
        ), executor.last());
    }

    @Test
    void createManyMixedBuildsItems() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.createMany(CreateManyOptions.builder("order", List.of(
                new CreateItem("a", bytes("pa"), "p1"),
                new CreateItem("b", bytes("pb"), "p2")))
            .nowMs(100)
            .independent(true)
            .build());

        assertArgs(List.of(
            "FLOW.CREATE_MANY", "MIXED", "TYPE", "order", "STATE", "queued", "NOW", 100L,
            "RUN_AT", 100L, "INDEPENDENT", "true", "ITEMS",
            "a", "p1", bytes("pa"), "b", "p2", bytes("pb")
        ), executor.last());
    }

    @Test
    void createManyMixedRequiresPartitionKey() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        IllegalArgumentException err = assertThrows(IllegalArgumentException.class, () ->
            client.createMany(CreateManyOptions.builder("order", List.of(
                new CreateItem("a", bytes("pa"), "p1"),
                new CreateItem("b", bytes("pb")))).build()));

        assertEquals("mixed createMany items require partition key", err.getMessage());
    }

    @Test
    void claimDueDecodesResp3Maps() {
        Map<Object, Object> record = Resp.testMap(
            "id", bytes("flow-1"),
            "type", "order",
            "state", "created",
            "partition_key", "p1",
            "payload", bytes("payload"),
            "lease_token", "lease",
            "fencing_token", 7L,
            "version", bytes("3"),
            "parent_flow_id", "parent",
            "root_flow_id", "root",
            "correlation_id", "corr"
        );
        FakeExecutor executor = new FakeExecutor(List.of(record));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder("order", "worker-1")
            .state("created")
            .partitionKey("p1")
            .leaseMs(5000)
            .limit(100)
            .nowMs(100)
            .reclaimExpired(true)
            .reclaimRatio(10)
            .build());

        assertArgs(List.of(
            "FLOW.CLAIM_DUE", "order", "STATE", "created", "WORKER", "worker-1",
            "LEASE_MS", 5000L, "LIMIT", 100, "NOW", 100L,
            "PARTITION", "p1", "RECLAIM_EXPIRED", "true", "RECLAIM_RATIO", 10L
        ), executor.last());
        assertEquals(1, jobs.size());
        FlowRecord job = jobs.getFirst();
        assertEquals("flow-1", job.id());
        assertEquals("order", job.type());
        assertEquals("created", job.state());
        assertEquals("p1", job.partitionKey());
        assertArrayEquals(bytes("payload"), (byte[]) job.payload());
        assertEquals("lease", job.leaseToken());
        assertEquals(7L, job.fencingToken());
        assertEquals(3L, job.version());
        assertEquals("parent", job.parentFlowId());
        assertEquals("root", job.rootFlowId());
        assertEquals("corr", job.correlationId());
    }

    @Test
    void claimDueBuildsMultiStateAndPartitionScan() {
        FakeExecutor executor = new FakeExecutor(List.of());
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder("order", "worker-1")
            .states(List.of("created", "waiting"))
            .partitionKeys(List.of("p1", "p2"))
            .leaseMs(5000)
            .limit(100)
            .nowMs(100)
            .build());

        assertEquals(List.of(), jobs);
        assertArgs(List.of(
            "FLOW.CLAIM_DUE", "order", "STATE", "created", "STATE", "waiting",
            "WORKER", "worker-1", "LEASE_MS", 5000L, "LIMIT", 100, "NOW", 100L,
            "PARTITIONS", 2, "p1", "p2"
        ), executor.last());
    }

    @Test
    void completeManyMixedBuildsItems() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.completeMany(CompleteManyOptions.builder(List.of(
                new ClaimedItem("a", "la", 1, "p1"),
                new ClaimedItem("b", "lb", 2, "p2")))
            .result(bytes("ok"))
            .nowMs(100)
            .independent(true)
            .build());

        assertArgs(List.of(
            "FLOW.COMPLETE_MANY", "MIXED", "RESULT", bytes("ok"), "NOW", 100L,
            "INDEPENDENT", "true", "ITEMS",
            "a", "p1", "la", 1L, "b", "p2", "lb", 2L
        ), executor.last());
    }

    @Test
    void transitionBuildsCommand() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.transition(TransitionOptions.builder("flow-1", "created", "charged", "lease", 9)
            .partitionKey("p1")
            .payload(bytes("next"))
            .nowMs(100)
            .runAtMs(150)
            .priority(4)
            .build());

        assertArgs(List.of(
            "FLOW.TRANSITION", "flow-1", "created", "charged", "LEASE_TOKEN", "lease",
            "FENCING", 9L, "NOW", 100L, "PARTITION", "p1",
            "PAYLOAD", bytes("next"), "RUN_AT", 150L, "PRIORITY", 4L
        ), executor.last());
    }

    @Test
    void transitionManyBuildsMixedItems() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.transitionMany(TransitionManyOptions.builder("running", "done", List.of(
                new FencedItem("a", 1, "la", "p1"),
                new FencedItem("b", 2, "lb", "p2")))
            .payload(bytes("next"))
            .priority(5)
            .nowMs(100)
            .independent(true)
            .build());

        assertArgs(List.of(
            "FLOW.TRANSITION_MANY", "MIXED", "running", "done",
            "PAYLOAD", bytes("next"), "PRIORITY", 5L, "NOW", 100L, "INDEPENDENT", "true",
            "ITEMS", "a", "p1", "la", 1L, "b", "p2", "lb", 2L
        ), executor.last());
    }

    @Test
    void spawnChildrenBuildsItemsShape() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.spawnChildren(SpawnChildrenOptions.builder("parent-1", List.of(
                new ChildSpec("small", "resize", bytes("small")),
                new ChildSpec("large", "resize", bytes("large"))))
            .partitionKey("p1")
            .leaseToken("lease")
            .fencingToken(9)
            .nowMs(100)
            .build());

        assertArgs(List.of(
            "FLOW.SPAWN_CHILDREN", "parent-1", "GROUP", "default", "WAIT", "all", "NOW", 100L,
            "PARTITION", "p1", "LEASE_TOKEN", "lease", "FENCING", 9L,
            "ITEMS", "small", "resize", bytes("small"), "large", "resize", bytes("large")
        ), executor.last());
    }

    @Test
    void spawnChildrenMixedRequiresPartitionKey() {
        FakeExecutor executor = new FakeExecutor("OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        IllegalArgumentException err = assertThrows(IllegalArgumentException.class, () ->
            client.spawnChildren(SpawnChildrenOptions.builder("parent-1", List.of(
                    new ChildSpec("small", "resize", bytes("small"), "p1"),
                    new ChildSpec("large", "resize", bytes("large"))))
                .build()));

        assertEquals("mixed spawnChildren items require partition key", err.getMessage());
    }

    @Test
    void recordsRejectMalformedResp() {
        FerricStoreException err = assertThrows(FerricStoreException.class, () -> Resp.records("bad", new RawCodec()));
        assertEquals("expected RESP array, got String", err.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertArgs(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size(), "argument count");
        for (int i = 0; i < expected.size(); i++) {
            Object want = expected.get(i);
            Object got = actual.get(i);
            if (want instanceof byte[] wantBytes && got instanceof byte[] gotBytes) {
                assertArrayEquals(wantBytes, gotBytes, "arg " + i);
            } else {
                assertEquals(want, got, "arg " + i);
            }
        }
    }

    private static final class FakeExecutor implements RedisExecutor {
        private final Object response;
        private final List<List<Object>> calls = new ArrayList<>();

        private FakeExecutor(Object response) {
            this.response = response;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            return response;
        }

        private List<Object> last() {
            return calls.getLast();
        }
    }
}
