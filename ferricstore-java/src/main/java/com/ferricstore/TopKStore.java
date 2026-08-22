package com.ferricstore;

import java.util.List;

public final class TopKStore {
    private final FerricStoreClient client;

    TopKStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean reserve(String key, long k) {
        return reserve(key, k, TopKReserveOptions.builder().build());
    }

    public boolean reserve(String key, long k, TopKReserveOptions options) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("TOPK.RESERVE key must not be empty");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("TOPK.RESERVE k must be positive");
        }
        List<Object> command = CommandArgs.args("TOPK.RESERVE", key, k);
        if (options.width() != null) {
            command.add(options.width());
            command.add(options.depth());
        }
        return CommandArgs.ok(client.command(command));
    }

    public List<Object> add(String key, Object... elements) {
        List<Object> args = CommandArgs.args("TOPK.ADD", key);
        for (Object element : elements) {
            args.add(client.codec().encode(element));
        }
        return Resp.list(client.command(args));
    }

    public List<Object> query(String key, Object... elements) {
        List<Object> args = CommandArgs.args("TOPK.QUERY", key);
        for (Object element : elements) {
            args.add(client.codec().encode(element));
        }
        return Resp.list(client.command(args));
    }

    public Object info(String key) {
        return client.command("TOPK.INFO", key);
    }
}
