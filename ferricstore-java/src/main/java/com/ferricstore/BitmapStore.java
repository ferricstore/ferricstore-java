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

    public long bitcount(String key, Object... range) {
        java.util.List<Object> args = CommandArgs.args("BITCOUNT", key);
        args.addAll(java.util.List.of(range));
        return Resp.number(client.command(args));
    }

    public long bitpos(String key, int bit, Object... range) {
        java.util.List<Object> args = CommandArgs.args("BITPOS", key, bit);
        args.addAll(java.util.List.of(range));
        return Resp.number(client.command(args));
    }

    public long bitop(String operation, String destination, String... keys) {
        java.util.List<String> all = new java.util.ArrayList<>();
        all.add(destination);
        all.addAll(java.util.List.of(keys));
        HashSlot.requireSame("BITOP", all);
        java.util.List<Object> args = CommandArgs.args("BITOP", operation, destination);
        args.addAll(java.util.List.of(keys));
        return Resp.number(client.command(args));
    }
}
