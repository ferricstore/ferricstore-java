package com.ferricstore;

import java.util.List;
import java.util.Map;

public final class HashStore {
    private final FerricStoreClient client;

    HashStore(FerricStoreClient client) {
        this.client = client;
    }

    public long hset(String key, Map<String, ?> entries) {
        List<Object> args = CommandArgs.args("HSET", key);
        entries.forEach(
                (field, value) -> {
                    args.add(field);
                    args.add(client.codec().encode(value));
                });
        return Resp.number(client.command(args));
    }

    public Object hget(String key, String field) {
        Object value = client.command("HGET", key, field);
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }

    public long hdel(String key, String... fields) {
        List<Object> args = CommandArgs.args("HDEL", key);
        args.addAll(List.of(fields));
        return Resp.number(client.command(args));
    }

    public Object hgetall(String key) {
        return client.command("HGETALL", key);
    }

    public boolean hexists(String key, String field) {
        return Resp.number(client.command("HEXISTS", key, field)) == 1;
    }

    public List<Object> hkeys(String key) {
        return Resp.list(client.command("HKEYS", key));
    }

    public long hlen(String key) {
        return Resp.number(client.command("HLEN", key));
    }

    public long hincrBy(String key, String field, long value) {
        return Resp.number(client.command("HINCRBY", key, field, value));
    }
}
