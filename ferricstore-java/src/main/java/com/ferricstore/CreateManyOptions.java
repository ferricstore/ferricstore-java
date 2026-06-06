package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record CreateManyOptions(
    String partitionKey,
    List<CreateItem> items,
    String type,
    String state,
    long runAtMs,
    long nowMs,
    Long priority,
    Boolean idempotent,
    Boolean independent,
    Long retentionTtlMs,
    Map<String, ?> values,
    Map<String, String> valueRefs
) {
    public static Builder builder(String type, List<CreateItem> items) {
        return new Builder(type, items);
    }

    public static final class Builder {
        private final String type;
        private final List<CreateItem> items;
        private String partitionKey;
        private String state = "queued";
        private long runAtMs;
        private long nowMs;
        private Long priority;
        private Boolean idempotent;
        private Boolean independent;
        private Long retentionTtlMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(String type, List<CreateItem> items) {
            this.type = type;
            this.items = List.copyOf(items);
        }

        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder state(String value) { this.state = value; return this; }
        public Builder runAtMs(long value) { this.runAtMs = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder priority(long value) { this.priority = value; return this; }
        public Builder idempotent(boolean value) { this.idempotent = value; return this; }
        public Builder independent(boolean value) { this.independent = value; return this; }
        public Builder retentionTtlMs(long value) { this.retentionTtlMs = value; return this; }
        public Builder value(String name, Object value) { this.values.put(name, value); return this; }
        public Builder values(Map<String, ?> values) { this.values.putAll(values); return this; }
        public Builder valueRef(String name, String ref) { this.valueRefs.put(name, ref); return this; }
        public Builder valueRefs(Map<String, String> valueRefs) { this.valueRefs.putAll(valueRefs); return this; }

        public CreateManyOptions build() {
            return new CreateManyOptions(partitionKey, items, type, state, runAtMs, nowMs,
                priority, idempotent, independent, retentionTtlMs, Map.copyOf(values), Map.copyOf(valueRefs));
        }
    }
}
