package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record TransitionOptions(
        String id,
        String fromState,
        String toState,
        String leaseToken,
        long fencingToken,
        String partitionKey,
        Object payload,
        long runAtMs,
        long nowMs,
        Long priority,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        boolean returnRecord) {
    public TransitionOptions {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(
            String id, String fromState, String toState, String leaseToken, long fencingToken) {
        return new Builder(id, fromState, toState, leaseToken, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final String fromState;
        private final String toState;
        private final String leaseToken;
        private final long fencingToken;
        private String partitionKey;
        private Object payload;
        private long runAtMs;
        private long nowMs;
        private Long priority;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private boolean returnRecord;

        private Builder(
                String id, String fromState, String toState, String leaseToken, long fencingToken) {
            this.id = id;
            this.fromState = fromState;
            this.toState = toState;
            this.leaseToken = leaseToken;
            this.fencingToken = fencingToken;
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

        public Builder value(String name, Object value) {
            this.values.put(name, value);
            return this;
        }

        public Builder valueRef(String name, String ref) {
            this.valueRefs.put(name, ref);
            return this;
        }

        public Builder returnRecord(boolean value) {
            this.returnRecord = value;
            return this;
        }

        public TransitionOptions build() {
            return new TransitionOptions(
                    id,
                    fromState,
                    toState,
                    leaseToken,
                    fencingToken,
                    partitionKey,
                    payload,
                    runAtMs,
                    nowMs,
                    priority,
                    Map.copyOf(values),
                    Map.copyOf(valueRefs),
                    returnRecord);
        }
    }
}
