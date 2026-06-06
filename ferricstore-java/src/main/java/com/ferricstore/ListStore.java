package com.ferricstore;

import java.util.List;

public final class ListStore {
    private final FerricStoreClient client;

    ListStore(FerricStoreClient client) {
        this.client = client;
    }

    public long lpush(String key, Object... elements) {
        return push("LPUSH", key, elements);
    }

    public long rpush(String key, Object... elements) {
        return push("RPUSH", key, elements);
    }

    public Object lpop(String key) {
        return decode(client.command("LPOP", key));
    }

    public Object rpop(String key) {
        return decode(client.command("RPOP", key));
    }

    public List<Object> lrange(String key, long start, long stop) {
        return Resp.list(client.command("LRANGE", key, start, stop)).stream()
                .map(this::decode)
                .toList();
    }

    public long llen(String key) {
        return Resp.number(client.command("LLEN", key));
    }

    public Object blpop(List<String> keys, long timeoutSeconds) {
        List<Object> args = CommandArgs.args("BLPOP");
        args.addAll(keys);
        args.add(timeoutSeconds);
        return client.command(args);
    }

    private long push(String command, String key, Object... elements) {
        List<Object> args = CommandArgs.args(command, key);
        for (Object element : elements) {
            args.add(client.codec().encode(element));
        }
        return Resp.number(client.command(args));
    }

    private Object decode(Object value) {
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }
}
