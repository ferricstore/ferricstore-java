package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record TransitionManyOptions(
        String partitionKey,
        String fromState,
        String toState,
        List<FencedItem> items,
        Object payload,
        long runAtMs,
        long nowMs,
        Long priority,
        Boolean independent,
        Map<String, ?> values,
        Map<String, String> valueRefs) {
    public TransitionManyOptions {
        items = ImmutableCopies.list(items);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(String fromState, String toState, List<FencedItem> items) {
        return new Builder(fromState, toState, items);
    }

    public static final class Builder {
        private final String fromState;
        private final String toState;
        private final List<FencedItem> items;
        private String partitionKey;
        private Object payload;
        private long runAtMs;
        private long nowMs;
        private Long priority;
        private Boolean independent;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(String fromState, String toState, List<FencedItem> items) {
            this.fromState = fromState;
            this.toState = toState;
            this.items = List.copyOf(items);
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
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

        public Builder priority(long value) {
            this.priority = value;
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

        public TransitionManyOptions build() {
            return new TransitionManyOptions(
                    partitionKey,
                    fromState,
                    toState,
                    items,
                    payload,
                    runAtMs,
                    nowMs,
                    priority,
                    independent,
                    Map.copyOf(values),
                    Map.copyOf(valueRefs));
        }
    }
}
