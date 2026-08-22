package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record FailOptions(
        String id,
        String leaseToken,
        long fencingToken,
        String partitionKey,
        Object error,
        Object payload,
        Long ttlMs,
        long nowMs,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        FlowMutationFields mutationFields,
        boolean returnRecord) {
    public FailOptions {
        FlowValidation.requireText(id, "flow id");
        FlowValidation.requireText(leaseToken, "flow lease token");
        FlowValidation.requireFencingToken(fencingToken);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        mutationFields = mutationFields == null ? FlowMutationFields.empty() : mutationFields;
    }

    public static Builder builder(String id, String leaseToken, long fencingToken) {
        return new Builder(id, leaseToken, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final String leaseToken;
        private final long fencingToken;
        private String partitionKey;
        private Object error;
        private Object payload;
        private Long ttlMs;
        private long nowMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private FlowMutationFields mutationFields = FlowMutationFields.empty();
        private boolean returnRecord;

        private Builder(String id, String leaseToken, long fencingToken) {
            this.id = id;
            this.leaseToken = leaseToken;
            this.fencingToken = fencingToken;
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            return this;
        }

        public Builder error(Object value) {
            this.error = value;
            return this;
        }

        public Builder payload(Object value) {
            this.payload = value;
            return this;
        }

        public Builder ttlMs(long value) {
            this.ttlMs = value;
            return this;
        }

        public Builder nowMs(long value) {
            this.nowMs = value;
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

        public Builder mutationFields(FlowMutationFields value) {
            mutationFields = value;
            return this;
        }

        public Builder returnRecord(boolean value) {
            this.returnRecord = value;
            return this;
        }

        public FailOptions build() {
            return new FailOptions(
                    id,
                    leaseToken,
                    fencingToken,
                    partitionKey,
                    error,
                    payload,
                    ttlMs,
                    nowMs,
                    values,
                    valueRefs,
                    mutationFields,
                    returnRecord);
        }
    }
}
