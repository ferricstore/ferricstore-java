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
        Map<String, Object> raw) {
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
}
