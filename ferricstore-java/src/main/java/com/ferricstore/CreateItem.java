package com.ferricstore;

import java.util.Map;

public record CreateItem(
        String id,
        Object payload,
        String partitionKey,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        MaxActiveMs maxActiveMs) {
    public CreateItem {
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
    }

    public CreateItem(String id, Object payload) {
        this(id, payload, null, Map.of(), Map.of(), null);
    }

    public CreateItem(String id, Object payload, String partitionKey) {
        this(id, payload, partitionKey, Map.of(), Map.of(), null);
    }

    public CreateItem(
            String id,
            Object payload,
            String partitionKey,
            Map<String, ?> values,
            Map<String, String> valueRefs) {
        this(id, payload, partitionKey, values, valueRefs, null);
    }
}
