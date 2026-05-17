package com.ferricstore;

public record CreateItem(String id, byte[] payload, String partitionKey) {
    public CreateItem(String id, byte[] payload) {
        this(id, payload, null);
    }
}
