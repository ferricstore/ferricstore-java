package com.ferricstore;

import java.util.List;

public final class TDigestStore {
    private final FerricStoreClient client;

    TDigestStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean create(String key, Object... options) {
        List<Object> args = CommandArgs.args("TDIGEST.CREATE", key);
        args.addAll(List.of(options));
        return CommandArgs.ok(client.command(args));
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

    public boolean reset(String key) {
        return CommandArgs.ok(client.command("TDIGEST.RESET", key));
    }

    public List<Object> cdf(String key, double... values) {
        return doubles("TDIGEST.CDF", key, values);
    }

    public List<Object> rank(String key, double... values) {
        return doubles("TDIGEST.RANK", key, values);
    }

    public List<Object> reverseRank(String key, double... values) {
        return doubles("TDIGEST.REVRANK", key, values);
    }

    public List<Object> byRank(String key, long... ranks) {
        return longs("TDIGEST.BYRANK", key, ranks);
    }

    public List<Object> byReverseRank(String key, long... ranks) {
        return longs("TDIGEST.BYREVRANK", key, ranks);
    }

    public double trimmedMean(String key, double low, double high) {
        return Resp.decimal(client.command("TDIGEST.TRIMMED_MEAN", key, low, high));
    }

    public double min(String key) {
        return Resp.decimal(client.command("TDIGEST.MIN", key));
    }

    public double max(String key) {
        return Resp.decimal(client.command("TDIGEST.MAX", key));
    }

    public boolean merge(String destination, int numKeys, Object... sourceAndOptions) {
        List<Object> args = CommandArgs.args("TDIGEST.MERGE", destination, numKeys);
        args.addAll(List.of(sourceAndOptions));
        return CommandArgs.ok(client.command(args));
    }

    public Object info(String key) {
        return client.command("TDIGEST.INFO", key);
    }

    private List<Object> doubles(String command, String key, double... values) {
        List<Object> args = CommandArgs.args(command, key);
        for (double value : values) {
            args.add(value);
        }
        return Resp.list(client.command(args));
    }

    private List<Object> longs(String command, String key, long... values) {
        List<Object> args = CommandArgs.args(command, key);
        for (long value : values) {
            args.add(value);
        }
        return Resp.list(client.command(args));
    }
}
