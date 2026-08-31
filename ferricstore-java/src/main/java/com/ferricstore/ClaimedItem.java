package com.ferricstore;

import java.util.Map;

public record ClaimedItem(
        String id,
        String leaseToken,
        long fencingToken,
        String partitionKey,
        String type,
        String state,
        String runState,
        Object payload,
        Map<String, Object> attributes)
        implements ClaimedFlow {
    public ClaimedItem {
        attributes = ImmutableCopies.map(attributes);
    }

    public ClaimedItem(
            String id,
            String leaseToken,
            long fencingToken,
            String partitionKey,
            String type,
            String state,
            String runState,
            Object payload) {
        this(id, leaseToken, fencingToken, partitionKey, type, state, runState, payload, Map.of());
    }

    public ClaimedItem(String id, String leaseToken, long fencingToken, String partitionKey) {
        this(id, leaseToken, fencingToken, partitionKey, "", "running", null, null, Map.of());
    }
}
