package com.ferricstore;

import java.util.Map;

public record FlowRecord(
        String id,
        String type,
        String state,
        String partitionKey,
        Object payload,
        Object error,
        String failureReason,
        Long maxActiveMs,
        String leaseToken,
        long fencingToken,
        long version,
        String parentFlowId,
        String rootFlowId,
        String correlationId,
        Map<String, Object> values,
        Map<String, Object> valueRefs,
        Map<String, Object> attributes,
        Map<String, Object> stateMeta,
        Map<String, Object> raw)
        implements ClaimedFlow {
    public FlowRecord {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        attributes = ImmutableCopies.map(attributes);
        stateMeta = ImmutableCopies.map(stateMeta);
        raw = ImmutableCopies.map(raw);
    }

    public FlowRecord(
            String id,
            String type,
            String state,
            String partitionKey,
            Object payload,
            Object error,
            String failureReason,
            Long maxActiveMs,
            String leaseToken,
            long fencingToken,
            long version,
            String parentFlowId,
            String rootFlowId,
            String correlationId,
            Map<String, Object> values,
            Map<String, Object> valueRefs,
            Map<String, Object> raw) {
        this(
                id,
                type,
                state,
                partitionKey,
                payload,
                error,
                failureReason,
                maxActiveMs,
                leaseToken,
                fencingToken,
                version,
                parentFlowId,
                rootFlowId,
                correlationId,
                values,
                valueRefs,
                Map.of(),
                Map.of(),
                raw);
    }

    public FlowRecord(
            String id,
            String type,
            String state,
            String partitionKey,
            Object payload,
            String leaseToken,
            long fencingToken,
            long version,
            String parentFlowId,
            String rootFlowId,
            String correlationId,
            Map<String, Object> values,
            Map<String, Object> valueRefs,
            Map<String, Object> raw) {
        this(
                id,
                type,
                state,
                partitionKey,
                payload,
                null,
                null,
                null,
                leaseToken,
                fencingToken,
                version,
                parentFlowId,
                rootFlowId,
                correlationId,
                values,
                valueRefs,
                Map.of(),
                Map.of(),
                raw);
    }

    public <T> T payloadAs(Class<T> type) {
        return type.cast(payload);
    }

    /** Returns the logical workflow state for an actively claimed flow. */
    @Override
    public String runState() {
        Object value = raw.get("run_state");
        if (value instanceof byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return value == null ? null : String.valueOf(value);
    }
}
