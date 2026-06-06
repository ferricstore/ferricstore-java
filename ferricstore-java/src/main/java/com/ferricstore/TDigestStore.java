package com.ferricstore;

import java.util.List;

public final class TDigestStore {
    private final FerricStoreClient client;

    TDigestStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean create(String key) {
        return CommandArgs.ok(client.command("TDIGEST.CREATE", key));
    }

    public boolean add(String key, double... values) {
        List<Object> args = CommandArgs.args("TDIGEST.ADD", key);
        for (double value : values) {
            args.add(value);
        }
        return CommandArgs.ok(client.command(args));
    }

    public List<Object> quantile(String key, double... quantiles) {
        List<Object> args = CommandArgs.args("TDIGEST.QUANTILE", key);
        for (double quantile : quantiles) {
            args.add(quantile);
        }
        return Resp.list(client.command(args));
    }

    public Object info(String key) {
        return client.command("TDIGEST.INFO", key);
    }
}
