package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            .payload(true)
            .payloadMaxBytes(2048)
            .reclaimExpired(true)
            .reclaimRatio(10)
            .build());

        assertArgs(List.of(
            "FLOW.CLAIM_DUE", "order", "STATE", "created", "WORKER", "worker-1",
            "LEASE_MS", 5000L, "LIMIT", 100, "NOW", 100L,
            "PARTITION", "p1", "PAYLOAD", "MAXBYTES", 2048L,
            "RECLAIM_EXPIRED", "true", "RECLAIM_RATIO", 10L
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
    void getDecodesPairArrayRecords() {
        FakeExecutor executor = new FakeExecutor(List.of(
            List.of(bytes("id"), bytes("flow-1")),
            Map.entry("type", "order"),
            List.of("state", "completed"),
            List.of("partition_key", "p1"),
            List.of("payload", bytes("payload")),
            List.of("values", List.of(List.of("attempt", bytes("1")))),
            List.of("lease_token", "lease"),
            List.of("fencing_token", 7L),
            List.of("version", bytes("3"))
        ));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        FlowRecord record = client.get("flow-1", "p1");

        assertArgs(List.of("FLOW.GET", "flow-1", "PARTITION", "p1"), executor.last());
        assertEquals("flow-1", record.id());
        assertEquals("order", record.type());
        assertEquals("completed", record.state());
        assertEquals("p1", record.partitionKey());
        assertArrayEquals(bytes("payload"), (byte[]) record.payload());
        assertArrayEquals(bytes("1"), (byte[]) record.values().get("attempt"));
        assertEquals("lease", record.leaseToken());
        assertEquals(7L, record.fencingToken());
        assertEquals(3L, record.version());
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
    void claimJobsBuildsCompactReturnAndDecodesItems() {
        FakeExecutor executor = new FakeExecutor(List.of(
            List.of("flow-1", "p1", "lease-1", 9L, "created"),
            Resp.testMap("id", "flow-2", "partition_key", "p2", "lease_token", "lease-2", "fencing_token", "10")
        ));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        List<ClaimedItem> jobs = client.claimJobs(ClaimDueOptions.builder("order", "worker-1")
            .state("created")
            .limit(2)
            .nowMs(100)
            .includeState(true)
            .build());

        assertArgs(List.of(
            "FLOW.CLAIM_DUE", "order", "STATE", "created", "WORKER", "worker-1",
            "LEASE_MS", 30000L, "LIMIT", 2, "NOW", 100L, "RETURN", "JOBS_COMPACT_STATE"
        ), executor.last());
        assertEquals(List.of(
            new ClaimedItem("flow-1", "lease-1", 9L, "p1", "", "running", "created", null),
            new ClaimedItem("flow-2", "lease-2", 10L, "p2")
        ), jobs);
    }

    @Test
    void reclaimBuildsRunningLeaseCommandShape() {
        FakeExecutor executor = new FakeExecutor(List.of(
            Resp.testMap("id", "flow-1", "lease_token", "lease-1", "fencing_token", 3L)
        ));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        List<FlowRecord> jobs = client.reclaim(ClaimDueOptions.builder("order", "worker-1")
            .partitionKey("p1")
            .leaseMs(5000)
            .limit(10)
            .nowMs(100)
            .payload(false)
            .build());

        assertArgs(List.of(
            "FLOW.RECLAIM", "order", "WORKER", "worker-1", "LEASE_MS", 5000L,
            "LIMIT", 10, "NOW", 100L, "PARTITION", "p1", "NOPAYLOAD"
        ), executor.last());
        assertEquals(1, jobs.size());
        assertEquals("flow-1", jobs.getFirst().id());
        assertEquals("lease-1", jobs.getFirst().leaseToken());
        assertEquals(3L, jobs.getFirst().fencingToken());
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

    @Test
    void optionalRecordTreatsEmptyArrayAsMissing() {
        assertNull(Resp.optionalRecord(List.of(), new RawCodec()));
    }

    @Test
    void optionalRecordTreatsEmptyMapAsMissing() {
        assertNull(Resp.optionalRecord(Map.of(), new RawCodec()));
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
