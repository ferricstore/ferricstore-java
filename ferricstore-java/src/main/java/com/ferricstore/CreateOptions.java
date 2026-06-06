package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record CreateOptions(
        String id,
        String type,
        String state,
        Object payload,
        String partitionKey,
        String parentFlowId,
        String rootFlowId,
        String correlationId,
        long runAtMs,
        long nowMs,
        Long priority,
        Boolean idempotent,
        Long retentionTtlMs,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        boolean returnRecord) {
    public CreateOptions {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(String id, String type) {
        return new Builder(id, type);
    }

    public static final class Builder {
        private final String id;
        private final String type;
        private String state = "queued";
        private Object payload;
        private String partitionKey;
        private String parentFlowId;
        private String rootFlowId;
        private String correlationId;
        private long runAtMs;
        private long nowMs;
        private Long priority;
        private Boolean idempotent;
        private Long retentionTtlMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private boolean returnRecord;

        private Builder(String id, String type) {
            this.id = id;
            this.type = type;
        }

        public Builder state(String value) {
            this.state = value;
            return this;
        }

        public Builder payload(Object value) {
            this.payload = value;
            return this;
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            return this;
        }

        public Builder parentFlowId(String value) {
            this.parentFlowId = value;
            return this;
        }

        public Builder rootFlowId(String value) {
            this.rootFlowId = value;
            return this;
        }

        public Builder correlationId(String value) {
            this.correlationId = value;
            return this;
        }

        public Builder runAtMs(long value) {
            this.runAtMs = value;
            return this;
        }

        public Builder nowMs(long value) {
            this.nowMs = value;
            return this;
        }

        public Builder priority(long value) {
            this.priority = value;
            return this;
        }

        public Builder idempotent(boolean value) {
            this.idempotent = value;
            return this;
        }

        public Builder retentionTtlMs(long value) {
            this.retentionTtlMs = value;
            return this;
        }

        public Builder value(String name, Object value) {
            this.values.put(name, value);
            return this;
        }

        public Builder values(Map<String, ?> values) {
            this.values.putAll(values);
            return this;
        }

        public Builder valueRef(String name, String ref) {
            this.valueRefs.put(name, ref);
            return this;
        }

        public Builder valueRefs(Map<String, String> valueRefs) {
            this.valueRefs.putAll(valueRefs);
            return this;
        }

        public Builder returnRecord(boolean value) {
            this.returnRecord = value;
            return this;
        }

        public CreateOptions build() {
            return new CreateOptions(
                    id,
                    type,
                    state,
                    payload,
                    partitionKey,
                    parentFlowId,
                    rootFlowId,
                    correlationId,
                    runAtMs,
                    nowMs,
                    priority,
                    idempotent,
                    retentionTtlMs,
                    Map.copyOf(values),
                    Map.copyOf(valueRefs),
                    returnRecord);
        }
    }
}
