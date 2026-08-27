package com.ferricstore;

import java.util.Map;

public record ChildSpec(
        String id,
        String type,
        Object payload,
        String partitionKey,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        MaxActiveMs maxActiveMs) {
    public ChildSpec {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public ChildSpec(String id, String type, Object payload) {
        this(id, type, payload, null, Map.of(), Map.of(), null);
    }

    public ChildSpec(String id, String type, Object payload, String partitionKey) {
        this(id, type, payload, partitionKey, Map.of(), Map.of(), null);
    }

    public ChildSpec(
            String id,
            String type,
            Object payload,
            String partitionKey,
            Map<String, ?> values,
            Map<String, String> valueRefs) {
        this(id, type, payload, partitionKey, values, valueRefs, null);
    }
}
