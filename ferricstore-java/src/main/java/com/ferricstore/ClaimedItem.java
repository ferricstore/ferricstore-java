package com.ferricstore;

public record ClaimedItem(
        String id,
        String leaseToken,
        long fencingToken,
        String partitionKey,
        String type,
        String state,
        String runState,
        Object payload) {
    public ClaimedItem(String id, String leaseToken, long fencingToken, String partitionKey) {
        this(id, leaseToken, fencingToken, partitionKey, "", "running", null, null);
    }
}
