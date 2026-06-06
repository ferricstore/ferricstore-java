package com.ferricstore;

import java.util.List;

public record CompleteManyOptions(String partitionKey, List<ClaimedItem> items, Object result, Object payload, Long ttlMs, long nowMs, Boolean independent) {
    public static Builder builder(List<ClaimedItem> items) { return new Builder(items); }
    public static final class Builder {
        private final List<ClaimedItem> items; private String partitionKey; private Object result; private Object payload; private Long ttlMs; private long nowMs; private Boolean independent;
        private Builder(List<ClaimedItem> items) { this.items = List.copyOf(items); }
        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder result(Object value) { this.result = value; return this; }
        public Builder payload(Object value) { this.payload = value; return this; }
        public Builder ttlMs(long value) { this.ttlMs = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder independent(boolean value) { this.independent = value; return this; }
        public CompleteManyOptions build() { return new CompleteManyOptions(partitionKey, items, result, payload, ttlMs, nowMs, independent); }
    }
}
