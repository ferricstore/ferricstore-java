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

    public List<Object> lpop(String key, long count) {
        return Resp.list(client.command("LPOP", key, count)).stream().map(this::decode).toList();
    }

    public Object rpop(String key) {
        return decode(client.command("RPOP", key));
    }

    public List<Object> rpop(String key, long count) {
        return Resp.list(client.command("RPOP", key, count)).stream().map(this::decode).toList();
    }

    public List<Object> lrange(String key, long start, long stop) {
        return Resp.list(client.command("LRANGE", key, start, stop)).stream()
                .map(this::decode)
                .toList();
    }

    public long llen(String key) {
        return Resp.number(client.command("LLEN", key));
    }

    public Object lindex(String key, long index) {
        return decode(client.command("LINDEX", key, index));
    }

    public boolean lset(String key, long index, Object value) {
        return CommandArgs.ok(client.command("LSET", key, index, client.codec().encode(value)));
    }

    public long lrem(String key, long count, Object value) {
        return Resp.number(client.command("LREM", key, count, client.codec().encode(value)));
    }

    public boolean ltrim(String key, long start, long stop) {
        return CommandArgs.ok(client.command("LTRIM", key, start, stop));
    }

    public Object lpos(String key, Object value, Object... options) {
        List<Object> args = CommandArgs.args("LPOS", key, client.codec().encode(value));
        args.addAll(List.of(options));
        return client.command(args);
    }

    public long linsert(String key, String where, Object pivot, Object value) {
        String normalized = where.toUpperCase(java.util.Locale.ROOT);
        if (!"BEFORE".equals(normalized) && !"AFTER".equals(normalized)) {
            throw new IllegalArgumentException("where must be BEFORE or AFTER");
        }
        return Resp.number(
                client.command(
                        "LINSERT",
                        key,
                        normalized,
                        client.codec().encode(pivot),
                        client.codec().encode(value)));
    }

    public Object lmove(String source, String destination, String from, String to) {
        return decode(client.command("LMOVE", source, destination, from, to));
    }

    public Object blpop(List<String> keys, long timeoutSeconds) {
        List<Object> args = CommandArgs.args("BLPOP");
        args.addAll(keys);
        args.add(timeoutSeconds);
        return client.command(args);
    }

    public Object brpop(List<String> keys, long timeoutSeconds) {
        List<Object> args = CommandArgs.args("BRPOP");
        args.addAll(keys);
        args.add(timeoutSeconds);
        return client.command(args);
    }

    public Object blmove(
            String source, String destination, String from, String to, double timeoutSeconds) {
        return client.command("BLMOVE", source, destination, from, to, timeoutSeconds);
    }

    public Object blmpop(
            double timeoutSeconds, List<String> keys, String direction, Integer count) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("BLMPOP requires at least one key");
        }
        List<Object> args = CommandArgs.args("BLMPOP", timeoutSeconds, keys.size());
        args.addAll(keys);
        args.add(direction);
        CommandArgs.append(args, "COUNT", count);
        return client.command(args);
    }

    public long lpushx(String key, Object... elements) {
        return push("LPUSHX", key, elements);
    }

    public long rpushx(String key, Object... elements) {
        return push("RPUSHX", key, elements);
    }

    public Object rpoplpush(String source, String destination) {
        return decode(client.command("RPOPLPUSH", source, destination));
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
