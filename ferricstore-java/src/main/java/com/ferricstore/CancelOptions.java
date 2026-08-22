package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record CancelOptions(
        String id,
        long fencingToken,
        String leaseToken,
        String partitionKey,
        Object reason,
        Long ttlMs,
        long nowMs,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        FlowMutationFields mutationFields,
        boolean returnRecord) {
    public CancelOptions {
        FlowValidation.requireText(id, "flow id");
        FlowValidation.requireFencingToken(fencingToken);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        mutationFields = mutationFields == null ? FlowMutationFields.empty() : mutationFields;
    }

    public static Builder builder(String id, long fencingToken) {
        return new Builder(id, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final long fencingToken;
        private String leaseToken;
        private String partitionKey;
        private Object reason;
        private Long ttlMs;
        private long nowMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private FlowMutationFields mutationFields = FlowMutationFields.empty();
        private boolean returnRecord;

        private Builder(String id, long fencingToken) {
            this.id = id;
            this.fencingToken = fencingToken;
        }

        public Builder leaseToken(String value) {
            this.leaseToken = value;
            return this;
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            return this;
        }

        public Builder reason(Object value) {
            this.reason = value;
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

        public CancelOptions build() {
            return new CancelOptions(
                    id,
                    fencingToken,
                    leaseToken,
                    partitionKey,
                    reason,
                    ttlMs,
                    nowMs,
                    values,
                    valueRefs,
                    mutationFields,
                    returnRecord);
        }
    }
}
