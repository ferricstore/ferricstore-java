package com.ferricstore;

import java.util.List;

public final class CountMinSketchStore {
    private final FerricStoreClient client;

    CountMinSketchStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean initByDim(String key, long width, long depth) { return CommandArgs.ok(client.command("CMS.INITBYDIM", key, width, depth)); }
    public boolean initByProb(String key, double error, double probability) { return CommandArgs.ok(client.command("CMS.INITBYPROB", key, error, probability)); }
    public List<Object> incrBy(String key, Object item, long count) { return Resp.list(client.command("CMS.INCRBY", key, client.codec().encode(item), count)); }
    public List<Object> query(String key, Object... items) {
        List<Object> args = CommandArgs.args("CMS.QUERY", key);
        for (Object item : items) args.add(client.codec().encode(item));
        return Resp.list(client.command(args));
    }
    public Object info(String key) { return client.command("CMS.INFO", key); }
}
