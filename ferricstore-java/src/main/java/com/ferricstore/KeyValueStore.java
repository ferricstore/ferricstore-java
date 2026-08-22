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

    public double incrByFloat(String key, double by) {
        return Double.parseDouble(Resp.string(client.command("INCRBYFLOAT", key, by)));
    }

    public long append(String key, Object value) {
        return Resp.number(client.command("APPEND", key, client.codec().encode(value)));
    }

    public long strlen(String key) {
        return Resp.number(client.command("STRLEN", key));
    }

    public Object getdel(String key) {
        return decode(client.command("GETDEL", key));
    }

    public Object getex(String key, Object... options) {
        List<Object> args = CommandArgs.args("GETEX", key);
        args.addAll(List.of(options));
        return decode(client.command(args));
    }

    public boolean setnx(String key, Object value) {
        return Resp.number(client.command("SETNX", key, client.codec().encode(value))) == 1;
    }

    public boolean setex(String key, long seconds, Object value) {
        return CommandArgs.ok(client.command("SETEX", key, seconds, client.codec().encode(value)));
    }

    public boolean psetex(String key, long milliseconds, Object value) {
        return CommandArgs.ok(
                client.command("PSETEX", key, milliseconds, client.codec().encode(value)));
    }

    public Object getrange(String key, long start, long end) {
        return client.command("GETRANGE", key, start, end);
    }

    public long setrange(String key, long offset, Object value) {
        return Resp.number(client.command("SETRANGE", key, offset, client.codec().encode(value)));
    }

    public boolean expire(String key, long seconds) {
        return Resp.number(client.command("EXPIRE", key, seconds)) == 1;
    }

    public boolean expire(String key, long seconds, String condition) {
        return expiry("EXPIRE", key, seconds, condition);
    }

    public boolean pexpire(String key, long milliseconds) {
        return expiry("PEXPIRE", key, milliseconds, null);
    }

    public boolean pexpire(String key, long milliseconds, String condition) {
        return expiry("PEXPIRE", key, milliseconds, condition);
    }

    public boolean expireAt(String key, long unixSeconds) {
        return expiry("EXPIREAT", key, unixSeconds, null);
    }

    public boolean expireAt(String key, long unixSeconds, String condition) {
        return expiry("EXPIREAT", key, unixSeconds, condition);
    }

    public boolean pexpireAt(String key, long unixMilliseconds) {
        return expiry("PEXPIREAT", key, unixMilliseconds, null);
    }

    public boolean pexpireAt(String key, long unixMilliseconds, String condition) {
        return expiry("PEXPIREAT", key, unixMilliseconds, condition);
    }

    public long ttl(String key) {
        return Resp.number(client.command("TTL", key));
    }

    public long pttl(String key) {
        return Resp.number(client.command("PTTL", key));
    }

    public boolean persist(String key) {
        return Resp.number(client.command("PERSIST", key)) == 1;
    }

    public long expireTime(String key) {
        return Resp.number(client.command("EXPIRETIME", key));
    }

    public long pexpireTime(String key) {
        return Resp.number(client.command("PEXPIRETIME", key));
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

    public long unlink(String... keys) {
        List<Object> args = CommandArgs.args("UNLINK");
        args.addAll(List.of(keys));
        return Resp.number(client.command(args));
    }

    public boolean rename(String key, String newKey) {
        return CommandArgs.ok(client.command("RENAME", key, newKey));
    }

    public boolean renamenx(String key, String newKey) {
        return Resp.number(client.command("RENAMENX", key, newKey)) == 1;
    }

    public boolean copy(String source, String destination, Object... options) {
        List<Object> args = CommandArgs.args("COPY", source, destination);
        args.addAll(List.of(options));
        return Resp.number(client.command(args)) == 1;
    }

    public String randomKey() {
        return Resp.optionalString(client.command("RANDOMKEY"));
    }

    public Object object(String subcommand, Object... args) {
        List<Object> command = CommandArgs.args("OBJECT", subcommand);
        command.addAll(List.of(args));
        return client.command(command);
    }

    private boolean expiry(String command, String key, long value, String condition) {
        List<Object> args = CommandArgs.args(command, key, value);
        if (condition != null) {
            String normalized = condition.toUpperCase(java.util.Locale.ROOT);
            if (!List.of("NX", "XX", "GT", "LT").contains(normalized)) {
                throw new IllegalArgumentException("expiry condition must be NX, XX, GT, or LT");
            }
            args.add(normalized);
        }
        return Resp.number(client.command(args)) == 1;
    }

    private Object decode(Object value) {
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
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
