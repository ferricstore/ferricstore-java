package com.ferricstore;

public record ClaimedItem(String id, String leaseToken, long fencingToken, String partitionKey) {
}
