package com.ferricstore;

import java.util.List;

public final class BloomFilterStore {
    private final FerricStoreClient client;

    BloomFilterStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean reserve(String key, double errorRate, long capacity) { return CommandArgs.ok(client.command("BF.RESERVE", key, errorRate, capacity)); }
    public boolean add(String key, Object element) { return Resp.number(client.command("BF.ADD", key, client.codec().encode(element))) == 1; }
    public List<Object> madd(String key, Object... elements) {
        List<Object> args = CommandArgs.args("BF.MADD", key);
        for (Object element : elements) args.add(client.codec().encode(element));
        return Resp.list(client.command(args));
    }
    public boolean exists(String key, Object element) { return Resp.number(client.command("BF.EXISTS", key, client.codec().encode(element))) == 1; }
    public long card(String key) { return Resp.number(client.command("BF.CARD", key)); }
    public Object info(String key) { return client.command("BF.INFO", key); }
}
