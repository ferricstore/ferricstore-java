package com.ferricstore;

import java.util.List;

public final class TopKStore {
    private final FerricStoreClient client;

    TopKStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean reserve(String key, long k) { return CommandArgs.ok(client.command("TOPK.RESERVE", key, k)); }
    public List<Object> add(String key, Object... elements) {
        List<Object> args = CommandArgs.args("TOPK.ADD", key);
        for (Object element : elements) args.add(client.codec().encode(element));
        return Resp.list(client.command(args));
    }
    public List<Object> query(String key, Object... elements) {
        List<Object> args = CommandArgs.args("TOPK.QUERY", key);
        for (Object element : elements) args.add(client.codec().encode(element));
        return Resp.list(client.command(args));
    }
    public Object info(String key) { return client.command("TOPK.INFO", key); }
}
