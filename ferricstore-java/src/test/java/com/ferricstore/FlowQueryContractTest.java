package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FlowQueryContractTest {
    @Test
    void exposesTheVersionedQueryEnvelopeWithoutLosingMetadata() {
        Map<String, Object> envelope =
                Map.of(
                        "version", "ferric.flow.query.result/v1",
                        "records", List.of(),
                        "page", Map.of("has_more", false),
                        "usage", Map.of("result_records", 0L));
        CapturingExecutor executor = new CapturingExecutor(envelope);
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        Map<String, Object> actual =
                client.flowQuery(
                        "FROM runs WHERE partition_key = @partition RETURN COUNT",
                        Map.of("partition", "tenant-a"));

        assertEquals(envelope, actual);
        assertEquals("FROM runs WHERE partition_key = @partition RETURN COUNT", executor.query);
        assertEquals(Map.of("partition", "tenant-a"), executor.params);
    }

    @Test
    void listUsesFlowQueryAndDecodesTheVersionedRecordPage() {
        CapturingExecutor executor = new CapturingExecutor(page("flow-1", "queued"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        List<FlowRecord> records = client.list("order", "queued", "tenant-a", 25);

        assertEquals(List.of("flow-1"), records.stream().map(FlowRecord::id).toList());
        assertEquals(
                "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
                        + "ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS",
                executor.query);
        assertEquals(
                Map.of("partition", "tenant-a", "type", "order", "state", "queued"),
                executor.params);
    }

    @Test
    void terminalFailureAndLineageHelpersUseSupportedQueryShapes() {
        CapturingExecutor executor = new CapturingExecutor(page("flow-1", "failed"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.terminals("order", null, "tenant-a", 0);
        assertEquals(
                "FROM runs WHERE partition_key = @partition AND type = @type "
                        + "AND state IN (@terminal_0, @terminal_1, @terminal_2) "
                        + "ORDER BY updated_at_ms ASC LIMIT 100 RETURN RECORDS",
                executor.query);
        assertEquals("completed", executor.params.get("terminal_0"));
        assertEquals("failed", executor.params.get("terminal_1"));
        assertEquals("cancelled", executor.params.get("terminal_2"));

        client.failures("order", "tenant-a", 20);
        assertEquals(
                "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
                        + "ORDER BY updated_at_ms ASC LIMIT 20 RETURN RECORDS",
                executor.query);
        assertEquals("failed", executor.params.get("state"));

        client.byParent("parent-1", "tenant-a", 10);
        assertLineage(executor, "parent_flow_id", "parent-1", "DESC");

        client.byRoot("root-1", "tenant-a", 10);
        assertLineage(executor, "root_flow_id", "root-1", "ASC");

        client.byCorrelation("correlation-1", "tenant-a", 10);
        assertLineage(executor, "correlation_id", "correlation-1", "DESC");
    }

    @Test
    void stuckUsesTheBoundedLeaseDeadlineShape() {
        CapturingExecutor executor = new CapturingExecutor(page("flow-1", "running"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.stuck("order", "tenant-a", 10, 1_000L, 5_000L);

        assertEquals(
                "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
                        + "AND lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms "
                        + "ORDER BY lease_deadline_ms ASC LIMIT 10 RETURN RECORDS",
                executor.query);
        assertEquals(
                Map.of(
                        "partition", "tenant-a",
                        "type", "order",
                        "state", "running",
                        "lease_from_ms", 0L,
                        "lease_to_ms", 4_000L),
                executor.params);
    }

    @Test
    void collectionHelpersRejectUnroutableOrUnboundedRequestsLocally() {
        CapturingExecutor executor = new CapturingExecutor(page("flow-1", "queued"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        assertThrows(IllegalArgumentException.class, () -> client.list("order", null, null, 10));
        assertThrows(
                IllegalArgumentException.class, () -> client.failures("order", "tenant-a", 101));
        assertThrows(
                IllegalArgumentException.class,
                () -> client.terminals("order", "queued", "tenant-a", 10));
        assertThrows(
                IllegalArgumentException.class,
                () -> client.stuck("order", "tenant-a", 10, -1L, 1_000L));
        assertEquals(0, executor.queries);
    }

    private static void assertLineage(
            CapturingExecutor executor, String field, String value, String direction) {
        assertEquals(
                "FROM runs WHERE partition_key = @partition AND "
                        + field
                        + " = @lineage_id ORDER BY updated_at_ms "
                        + direction
                        + " LIMIT 10 RETURN RECORDS",
                executor.query);
        assertEquals(Map.of("partition", "tenant-a", "lineage_id", value), executor.params);
    }

    private static Map<String, Object> page(String id, String state) {
        return Map.of(
                "version",
                "ferric.flow.query.result/v1",
                "records",
                List.of(Map.of("id", id, "type", "order", "state", state, "version", 1L)),
                "page",
                Map.of("has_more", false, "cursor", ""),
                "quality",
                Map.of(),
                "usage",
                Map.of("result_records", 1L));
    }

    private static final class CapturingExecutor implements CommandExecutor {
        private final Object response;
        private String query;
        private Map<String, Object> params = Map.of();
        private int queries;

        private CapturingExecutor(Object response) {
            this.response = response;
        }

        @Override
        public Object execute(List<Object> args) {
            throw new AssertionError("query helpers must use CommandExecutor.flowQuery");
        }

        @Override
        public Object flowQuery(String requestedQuery, Map<String, ?> requestedParams) {
            query = requestedQuery;
            params = new LinkedHashMap<>(requestedParams);
            queries++;
            return response;
        }
    }
}
