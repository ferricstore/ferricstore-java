package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record CompleteManyOptions(
        String partitionKey,
        List<ClaimedItem> items,
        Object result,
        Object payload,
        Long ttlMs,
        long nowMs,
        Boolean independent,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        FlowMutationFields mutationFields,
        boolean returnOkOnSuccess) {
    public CompleteManyOptions {
        items = ImmutableCopies.list(items);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        mutationFields = mutationFields == null ? FlowMutationFields.empty() : mutationFields;
    }

    public static Builder builder(List<ClaimedItem> items) {
        return new Builder(items);
    }

    public static final class Builder {
        private final List<ClaimedItem> items;
        private String partitionKey;
        private Object result;
        private Object payload;
        private Long ttlMs;
        private long nowMs;
        private Boolean independent;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private FlowMutationFields mutationFields = FlowMutationFields.empty();
        private boolean returnOkOnSuccess;

        private Builder(List<ClaimedItem> items) {
            this.items = List.copyOf(items);
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            return this;
        }

        public Builder result(Object value) {
            this.result = value;
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

        public Builder independent(boolean value) {
            this.independent = value;
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

        public Builder returnOkOnSuccess(boolean value) {
            returnOkOnSuccess = value;
            return this;
        }

        public CompleteManyOptions build() {
            return new CompleteManyOptions(
                    partitionKey,
                    items,
                    result,
                    payload,
                    ttlMs,
                    nowMs,
                    independent,
                    values,
                    valueRefs,
                    mutationFields,
                    returnOkOnSuccess);
        }
    }
}
