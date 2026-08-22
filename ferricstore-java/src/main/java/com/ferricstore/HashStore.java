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

    public List<Object> hmget(String key, String... fields) {
        List<Object> args = CommandArgs.args("HMGET", key);
        args.addAll(List.of(fields));
        return Resp.list(client.command(args)).stream().map(this::decode).toList();
    }

    public long hdel(String key, String... fields) {
        List<Object> args = CommandArgs.args("HDEL", key);
        args.addAll(List.of(fields));
        return Resp.number(client.command(args));
    }

    public Map<Object, Object> hgetall(String key) {
        Object response = client.command("HGETALL", key);
        Map<Object, Object> decoded = new java.util.LinkedHashMap<>();
        if (response instanceof Map<?, ?> map) {
            map.forEach((field, value) -> decoded.put(field, decode(value)));
            return decoded;
        }
        List<Object> values = Resp.list(response);
        if (values.size() % 2 != 0) {
            throw new FerricStoreException("HGETALL response must contain field/value pairs");
        }
        for (int index = 0; index < values.size(); index += 2) {
            decoded.put(values.get(index), decode(values.get(index + 1)));
        }
        return decoded;
    }

    public boolean hexists(String key, String field) {
        return Resp.number(client.command("HEXISTS", key, field)) == 1;
    }

    public List<Object> hkeys(String key) {
        return Resp.list(client.command("HKEYS", key));
    }

    public List<Object> hvals(String key) {
        return Resp.list(client.command("HVALS", key)).stream().map(this::decode).toList();
    }

    public long hlen(String key) {
        return Resp.number(client.command("HLEN", key));
    }

    public long hincrBy(String key, String field, long value) {
        return Resp.number(client.command("HINCRBY", key, field, value));
    }

    public double hincrByFloat(String key, String field, double value) {
        return Double.parseDouble(Resp.string(client.command("HINCRBYFLOAT", key, field, value)));
    }

    public boolean hsetnx(String key, String field, Object value) {
        return Resp.number(client.command("HSETNX", key, field, client.codec().encode(value))) == 1;
    }

    public long hstrlen(String key, String field) {
        return Resp.number(client.command("HSTRLEN", key, field));
    }

    public Object hrandfield(String key, Object... options) {
        return raw("HRANDFIELD", key, options);
    }

    public Object hscan(String key, long cursor, Object... options) {
        List<Object> args = CommandArgs.args("HSCAN", key, cursor);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public Object httl(String key, String... fields) {
        return fieldCommand("HTTL", key, null, fields);
    }

    public Object hpttl(String key, String... fields) {
        return fieldCommand("HPTTL", key, null, fields);
    }

    public Object hpersist(String key, String... fields) {
        return fieldCommand("HPERSIST", key, null, fields);
    }

    public Object hexpire(String key, long seconds, String... fields) {
        return fieldCommand("HEXPIRE", key, seconds, fields);
    }

    public Object hpexpire(String key, long milliseconds, String... fields) {
        return fieldCommand("HPEXPIRE", key, milliseconds, fields);
    }

    public Object hexpireTime(String key, String... fields) {
        return fieldCommand("HEXPIRETIME", key, null, fields);
    }

    public List<Object> hgetdel(String key, String... fields) {
        return Resp.list(fieldCommand("HGETDEL", key, null, fields)).stream()
                .map(this::decode)
                .toList();
    }

    public Object hgetex(String key, Object... args) {
        return raw("HGETEX", key, args);
    }

    public Object hsetex(String key, Object... args) {
        return raw("HSETEX", key, args);
    }

    private Object fieldCommand(String command, String key, Long duration, String... fields) {
        if (fields.length == 0) {
            throw new IllegalArgumentException(command + " requires at least one field");
        }
        List<Object> args = CommandArgs.args(command, key);
        if (duration != null) {
            args.add(duration);
        }
        args.add("FIELDS");
        args.add(fields.length);
        args.addAll(List.of(fields));
        return client.command(args);
    }

    private Object raw(String command, String key, Object... options) {
        List<Object> args = CommandArgs.args(command, key);
        args.addAll(List.of(options));
        return client.command(args);
    }

    private Object decode(Object value) {
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }
}
