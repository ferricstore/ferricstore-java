package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record RetryManyOptions(
        String partitionKey,
        List<ClaimedItem> items,
        Object error,
        Object payload,
        long runAtMs,
        long nowMs,
        Boolean independent,
        Map<String, ?> values,
        Map<String, String> valueRefs) {
    public RetryManyOptions {
        items = ImmutableCopies.list(items);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(List<ClaimedItem> items) {
        return new Builder(items);
    }

    public static final class Builder {
        private final List<ClaimedItem> items;
        private String partitionKey;
        private Object error;
        private Object payload;
        private long runAtMs;
        private long nowMs;
        private Boolean independent;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(List<ClaimedItem> items) {
            this.items = List.copyOf(items);
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

        public Builder runAtMs(long value) {
            this.runAtMs = value;
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
            this.values.put(name, value);
            return this;
        }

        public Builder valueRef(String name, String ref) {
            this.valueRefs.put(name, ref);
            return this;
        }

        public RetryManyOptions build() {
            return new RetryManyOptions(
                    partitionKey,
                    items,
                    error,
                    payload,
                    runAtMs,
                    nowMs,
                    independent,
                    Map.copyOf(values),
                    Map.copyOf(valueRefs));
        }
    }
}
