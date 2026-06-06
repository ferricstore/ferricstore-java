package com.ferricstore;

public final class BitmapStore {
    private final FerricStoreClient client;

    BitmapStore(FerricStoreClient client) {
        this.client = client;
    }

    public long setbit(String key, long offset, int value) {
        return Resp.number(client.command("SETBIT", key, offset, value));
    }

    public long getbit(String key, long offset) {
        return Resp.number(client.command("GETBIT", key, offset));
    }

    public long bitcount(String key) {
        return Resp.number(client.command("BITCOUNT", key));
    }
}
