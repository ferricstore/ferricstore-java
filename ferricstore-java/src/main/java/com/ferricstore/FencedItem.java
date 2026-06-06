package com.ferricstore;

public record FencedItem(String id, long fencingToken, String leaseToken, String partitionKey) {
    public FencedItem(String id, long fencingToken, String partitionKey) {
        this(id, fencingToken, null, partitionKey);
    }
}
