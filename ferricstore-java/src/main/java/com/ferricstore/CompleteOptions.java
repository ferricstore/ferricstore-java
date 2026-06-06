package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.Map;

public record CompleteOptions(
        String id,
        String leaseToken,
        long fencingToken,
        String partitionKey,
        Object result,
        Object payload,
        Long ttlMs,
        long nowMs,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        boolean returnRecord) {
    public CompleteOptions {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public static Builder builder(String id, String leaseToken, long fencingToken) {
        return new Builder(id, leaseToken, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final String leaseToken;
        private final long fencingToken;
        private String partitionKey;
        private Object result;
        private Object payload;
        private Long ttlMs;
        private long nowMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
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

        public CompleteOptions build() {
            return new CompleteOptions(
                    id,
                    leaseToken,
                    fencingToken,
                    partitionKey,
                    result,
                    payload,
                    ttlMs,
                    nowMs,
                    Map.copyOf(values),
                    Map.copyOf(valueRefs),
                    returnRecord);
        }
    }
}
