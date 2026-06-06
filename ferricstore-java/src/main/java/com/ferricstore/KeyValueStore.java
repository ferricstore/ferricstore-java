package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class KeyValueStore {
    private final FerricStoreClient client;

    KeyValueStore(FerricStoreClient client) {
        this.client = client;
    }

    public Object get(String key) {
        Object value = client.command("GET", key);
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }

    public <T> T get(String key, Class<T> type) {
        Object value = client.command("GET", key);
        return value instanceof byte[] bytes ? client.codec().decode(bytes, type) : type.cast(value);
    }

    public boolean set(String key, Object value) {
        return CommandArgs.ok(client.command("SET", key, client.codec().encode(value)));
    }

    public boolean set(String key, Object value, Long pxMs, Boolean nx) {
        List<Object> args = CommandArgs.args("SET", key, client.codec().encode(value));
        CommandArgs.append(args, "PX", pxMs);
        if (Boolean.TRUE.equals(nx)) {
            args.add("NX");
        }
        return CommandArgs.ok(client.command(args));
    }

    public long del(String... keys) {
        List<Object> args = CommandArgs.args("DEL");
        args.addAll(List.of(keys));
        return Resp.number(client.command(args));
    }

    public long exists(String... keys) {
        List<Object> args = CommandArgs.args("EXISTS");
        args.addAll(List.of(keys));
        return Resp.number(client.command(args));
    }

    public List<Object> mget(List<String> keys) {
        List<Object> args = CommandArgs.args("MGET");
        args.addAll(keys);
        return Resp.list(client.command(args)).stream()
            .map(item -> item instanceof byte[] bytes ? client.codec().decode(bytes) : item)
            .toList();
    }

    public boolean mset(Map<String, ?> entries) {
        List<Object> args = CommandArgs.args("MSET");
        entries.forEach((key, value) -> {
            args.add(key);
            args.add(client.codec().encode(value));
        });
        return CommandArgs.ok(client.command(args));
    }

    public long incr(String key) { return Resp.number(client.command("INCR", key)); }
    public long decr(String key) { return Resp.number(client.command("DECR", key)); }
    public long incrBy(String key, long by) { return Resp.number(client.command("INCRBY", key, by)); }
    public long decrBy(String key, long by) { return Resp.number(client.command("DECRBY", key, by)); }
    public boolean expire(String key, long seconds) { return Resp.number(client.command("EXPIRE", key, seconds)) == 1; }
    public long ttl(String key) { return Resp.number(client.command("TTL", key)); }
    public String type(String key) { return Resp.string(client.command("TYPE", key)); }
    public List<Object> keys(String pattern) { return Resp.list(client.command("KEYS", pattern)); }
    public Object scan(String cursor, String match, Long count) {
        List<Object> args = new ArrayList<>(List.of("SCAN", cursor));
        CommandArgs.append(args, "MATCH", match);
        CommandArgs.append(args, "COUNT", count);
        return client.command(args);
    }
}
