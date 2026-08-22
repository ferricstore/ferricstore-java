package com.ferricstore;

import java.util.ArrayList;
import java.util.Comparator;
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
        return value instanceof byte[] bytes
                ? client.codec().decode(bytes, type)
                : type.cast(value);
    }

    public boolean set(String key, Object value) {
        return Boolean.TRUE.equals(set(key, value, SetOptions.builder().build()));
    }

    public Object set(String key, Object value, SetOptions options) {
        List<Object> args = CommandArgs.args("SET", key, client.codec().encode(value));
        CommandArgs.append(args, "EX", options.exSeconds());
        CommandArgs.append(args, "PX", options.pxMilliseconds());
        CommandArgs.append(args, "EXAT", options.exatSeconds());
        CommandArgs.append(args, "PXAT", options.pxatMillis());
        if (options.nx()) {
            args.add("NX");
        }
        if (options.xx()) {
            args.add("XX");
        }
        if (options.get()) {
            args.add("GET");
        }
        if (options.keepTtl()) {
            args.add("KEEPTTL");
        }
        Object response = client.command(args);
        if (!options.get()) {
            return response == null ? null : CommandArgs.ok(response);
        }
        return response instanceof byte[] bytes ? client.codec().decode(bytes) : response;
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
        List<String> keys = validatedBulkKeys("MSET", entries);
        List<Object> args = CommandArgs.args("MSET");
        for (String key : keys) {
            args.add(key);
            args.add(encoded(entries.get(key), key));
        }
        return CommandArgs.ok(client.command(args));
    }

    public boolean msetnx(Map<String, ?> entries) {
        List<String> keys = validatedBulkKeys("MSETNX", entries);
        List<Object> args = CommandArgs.args("MSETNX");
        for (String key : keys) {
            args.add(key);
            args.add(encoded(entries.get(key), key));
        }
        Object response = client.command(args);
        return Boolean.TRUE.equals(response) || Resp.number(response) == 1;
    }

    public long incr(String key) {
        return Resp.number(client.command("INCR", key));
    }

    public long decr(String key) {
        return Resp.number(client.command("DECR", key));
    }

    public long incrBy(String key, long by) {
        return Resp.number(client.command("INCRBY", key, by));
    }

    public long decrBy(String key, long by) {
        return Resp.number(client.command("DECRBY", key, by));
    }

    public boolean expire(String key, long seconds) {
        return Resp.number(client.command("EXPIRE", key, seconds)) == 1;
    }

    public long ttl(String key) {
        return Resp.number(client.command("TTL", key));
    }

    public String type(String key) {
        return Resp.string(client.command("TYPE", key));
    }

    public List<Object> keys(String pattern) {
        return Resp.list(client.command("KEYS", pattern));
    }

    public Object scan(String cursor, String match, Long count) {
        List<Object> args = new ArrayList<>(List.of("SCAN", cursor));
        CommandArgs.append(args, "MATCH", match);
        CommandArgs.append(args, "COUNT", count);
        return client.command(args);
    }

    private List<String> validatedBulkKeys(String command, Map<String, ?> entries) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException(command + " requires at least one key/value pair");
        }
        List<String> keys = new ArrayList<>(entries.keySet());
        keys.sort(Comparator.naturalOrder());
        HashSlot.requireSame(command, keys);
        return keys;
    }

    private byte[] encoded(Object value, String key) {
        byte[] encoded = client.codec().encode(value);
        if (encoded == null) {
            throw new IllegalArgumentException("value for key " + key + " must not encode to null");
        }
        return encoded;
    }
}
