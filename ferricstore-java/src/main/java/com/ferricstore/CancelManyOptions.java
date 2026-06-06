package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record CancelManyOptions(
    String partitionKey,
    List<FencedItem> items,
    Object reason,
    Long ttlMs,
    long nowMs,
    Boolean independent,
    Map<String, ?> values,
    Map<String, String> valueRefs
) {
    public static Builder builder(List<FencedItem> items) { return new Builder(items); }

    public static final class Builder {
        private final List<FencedItem> items;
        private String partitionKey;
        private Object reason;
        private Long ttlMs;
        private long nowMs;
        private Boolean independent;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(List<FencedItem> items) { this.items = List.copyOf(items); }

        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder reason(Object value) { this.reason = value; return this; }
        public Builder ttlMs(long value) { this.ttlMs = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder independent(boolean value) { this.independent = value; return this; }
        public Builder value(String name, Object value) { this.values.put(name, value); return this; }
        public Builder valueRef(String name, String ref) { this.valueRefs.put(name, ref); return this; }

        public CancelManyOptions build() {
            return new CancelManyOptions(partitionKey, items, reason, ttlMs, nowMs,
                independent, Map.copyOf(values), Map.copyOf(valueRefs));
        }
    }
}
