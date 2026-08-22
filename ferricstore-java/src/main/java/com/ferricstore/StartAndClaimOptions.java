package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record StartAndClaimOptions(
        String id,
        String type,
        String initialState,
        String worker,
        long leaseMs,
        Object payload,
        String partitionKey,
        String parentFlowId,
        String rootFlowId,
        String correlationId,
        long nowMs,
        Long priority,
        Long retentionTtlMs,
        MaxActiveMs maxActiveMs,
        Map<String, ?> attributes,
        Map<String, ?> stateMeta,
        Map<String, ?> values,
        Map<String, String> valueRefs) {
    public StartAndClaimOptions {
        requireText(id, "id");
        requireText(type, "type");
        requireText(initialState, "initialState");
        requireText(worker, "worker");
        if (leaseMs <= 0) {
            throw new IllegalArgumentException("leaseMs must be positive");
        }
        attributes = ImmutableCopies.map(attributes);
        stateMeta = ImmutableCopies.map(stateMeta);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(String id, String type, String initialState, String worker) {
        return new Builder(id, type, initialState, worker);
    }

    public static final class Builder {
        private final String id;
        private final String type;
        private final String initialState;
        private final String worker;
        private long leaseMs = 30_000;
        private Object payload;
        private String partitionKey;
        private String parentFlowId;
        private String rootFlowId;
        private String correlationId;
        private long nowMs;
        private Long priority;
        private Long retentionTtlMs;
        private MaxActiveMs maxActiveMs;
        private final Map<String, Object> attributes = new LinkedHashMap<>();
        private final Map<String, Object> stateMeta = new LinkedHashMap<>();
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(String id, String type, String initialState, String worker) {
            this.id = id;
            this.type = type;
            this.initialState = initialState;
            this.worker = worker;
        }

        public Builder leaseMs(long value) {
            leaseMs = value;
            return this;
        }

        public Builder payload(Object value) {
            payload = value;
            return this;
        }

        public Builder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public Builder parentFlowId(String value) {
            parentFlowId = value;
            return this;
        }

        public Builder rootFlowId(String value) {
            rootFlowId = value;
            return this;
        }

        public Builder correlationId(String value) {
            correlationId = value;
            return this;
        }

        public Builder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public Builder priority(long value) {
            priority = value;
            return this;
        }

        public Builder retentionTtlMs(long value) {
            retentionTtlMs = value;
            return this;
        }

        public Builder maxActiveMs(long value) {
            maxActiveMs = MaxActiveMs.of(value);
            return this;
        }

        public Builder maxActiveMs(MaxActiveMs value) {
            maxActiveMs = value;
            return this;
        }

        public Builder attribute(String name, Object value) {
            attributes.put(name, value);
            return this;
        }

        public Builder stateMeta(String state, Object value) {
            stateMeta.put(state, value);
            return this;
        }

        public Builder value(String name, Object value) {
            values.put(name, value);
            return this;
        }

        public Builder valueRef(String name, String value) {
            valueRefs.put(name, value);
            return this;
        }

        public StartAndClaimOptions build() {
            return new StartAndClaimOptions(
                    id,
                    type,
                    initialState,
                    worker,
                    leaseMs,
                    payload,
                    partitionKey,
                    parentFlowId,
                    rootFlowId,
                    correlationId,
                    nowMs,
                    priority,
                    retentionTtlMs,
                    maxActiveMs,
                    attributes,
                    stateMeta,
                    values,
                    valueRefs);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
