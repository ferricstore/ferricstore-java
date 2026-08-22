package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class FlowQueries {
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 100;
    private static final Set<String> TERMINAL_STATES = Set.of("completed", "failed", "cancelled");

    private FlowQueries() {}

    static Request list(String type, String state, String partitionKey, int count) {
        String checkedType = required(type, "type");
        String checkedPartition = required(partitionKey, "partitionKey");
        String effectiveState = state == null ? "queued" : required(state, "state");
        Map<String, Object> params = common(checkedPartition, checkedType);
        params.put("state", effectiveState);
        return new Request(
                "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
                        + order("updated_at_ms", "ASC", count),
                params);
    }

    static Request terminals(String type, String state, String partitionKey, int count) {
        String checkedType = required(type, "type");
        String checkedPartition = required(partitionKey, "partitionKey");
        Map<String, Object> params = common(checkedPartition, checkedType);
        String statePredicate;
        if (state == null || "any".equals(state)) {
            statePredicate = "state IN (@terminal_0, @terminal_1, @terminal_2)";
            params.put("terminal_0", "completed");
            params.put("terminal_1", "failed");
            params.put("terminal_2", "cancelled");
        } else {
            String checkedState = required(state, "state");
            if (!TERMINAL_STATES.contains(checkedState)) {
                throw new IllegalArgumentException(
                        "state must be completed, failed, cancelled, any, or null");
            }
            statePredicate = "state = @state";
            params.put("state", checkedState);
        }
        return new Request(
                "FROM runs WHERE partition_key = @partition AND type = @type AND "
                        + statePredicate
                        + " "
                        + order("updated_at_ms", "ASC", count),
                params);
    }

    static Request failures(String type, String partitionKey, int count) {
        return list(type, "failed", partitionKey, count);
    }

    static Request lineage(
            String field, String id, String partitionKey, int count, String direction) {
        if (!List.of("parent_flow_id", "root_flow_id", "correlation_id").contains(field)) {
            throw new IllegalArgumentException("unsupported lineage field");
        }
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("partition", required(partitionKey, "partitionKey"));
        params.put("lineage_id", required(id, "lineageId"));
        return new Request(
                "FROM runs WHERE partition_key = @partition AND "
                        + field
                        + " = @lineage_id "
                        + order("updated_at_ms", direction, count),
                params);
    }

    static Request stuck(
            String type, String partitionKey, int count, long olderThanMs, long nowMs) {
        if (olderThanMs < 0 || nowMs < 0) {
            throw new IllegalArgumentException("stuck query times must be non-negative");
        }
        Map<String, Object> params =
                common(required(partitionKey, "partitionKey"), required(type, "type"));
        params.put("state", "running");
        params.put("lease_from_ms", 0L);
        params.put("lease_to_ms", Math.max(0L, nowMs - olderThanMs));
        return new Request(
                "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
                        + "AND lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms "
                        + order("lease_deadline_ms", "ASC", count),
                params);
    }

    private static Map<String, Object> common(String partitionKey, String type) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("partition", partitionKey);
        params.put("type", type);
        return params;
    }

    private static String order(String field, String direction, int requestedLimit) {
        int limit = limit(requestedLimit);
        return "ORDER BY " + field + " " + direction + " LIMIT " + limit + " RETURN RECORDS";
    }

    private static int limit(int count) {
        if (count == 0) {
            return DEFAULT_LIMIT;
        }
        if (count < 0 || count > MAX_LIMIT) {
            throw new IllegalArgumentException("count must be between 1 and 100, or 0 for default");
        }
        return count;
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    record Request(String query, Map<String, Object> params) {
        Request {
            params = Map.copyOf(params);
        }
    }
}
