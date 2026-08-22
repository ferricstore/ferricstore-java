package com.ferricstore;

import java.util.List;

public final class SetStore {
    private final FerricStoreClient client;

    SetStore(FerricStoreClient client) {
        this.client = client;
    }

    public long sadd(String key, Object... members) {
        List<Object> args = CommandArgs.args("SADD", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.number(client.command(args));
    }

    public long srem(String key, Object... members) {
        List<Object> args = CommandArgs.args("SREM", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.number(client.command(args));
    }

    public List<Object> smembers(String key) {
        return Resp.list(client.command("SMEMBERS", key)).stream()
                .map(item -> item instanceof byte[] bytes ? client.codec().decode(bytes) : item)
                .toList();
    }

    public boolean sismember(String key, Object member) {
        return Resp.number(client.command("SISMEMBER", key, client.codec().encode(member))) == 1;
    }

    public List<Boolean> smismember(String key, Object... members) {
        List<Object> args = CommandArgs.args("SMISMEMBER", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.list(client.command(args)).stream()
                .map(value -> Resp.number(value) == 1)
                .toList();
    }

    public long scard(String key) {
        return Resp.number(client.command("SCARD", key));
    }

    public List<Object> sinter(String... keys) {
        return members("SINTER", keys);
    }

    public List<Object> sunion(String... keys) {
        return members("SUNION", keys);
    }

    public List<Object> sdiff(String... keys) {
        return members("SDIFF", keys);
    }

    public long sdiffstore(String destination, String... keys) {
        return store("SDIFFSTORE", destination, keys);
    }

    public long sinterstore(String destination, String... keys) {
        return store("SINTERSTORE", destination, keys);
    }

    public long sunionstore(String destination, String... keys) {
        return store("SUNIONSTORE", destination, keys);
    }

    public long sintercard(List<String> keys, Integer limit) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("SINTERCARD requires at least one key");
        }
        HashSlot.requireSame("SINTERCARD", keys);
        List<Object> args = CommandArgs.args("SINTERCARD", keys.size());
        args.addAll(keys);
        CommandArgs.append(args, "LIMIT", limit);
        return Resp.number(client.command(args));
    }

    public Object srandmember(String key, Integer count) {
        Object response =
                count == null
                        ? client.command("SRANDMEMBER", key)
                        : client.command("SRANDMEMBER", key, count);
        return count == null
                ? decode(response)
                : Resp.list(response).stream().map(this::decode).toList();
    }

    public Object spop(String key, Integer count) {
        Object response =
                count == null ? client.command("SPOP", key) : client.command("SPOP", key, count);
        return count == null
                ? decode(response)
                : Resp.list(response).stream().map(this::decode).toList();
    }

    public boolean smove(String source, String destination, Object member) {
        HashSlot.requireSame("SMOVE", List.of(source, destination));
        return Resp.number(
                        client.command("SMOVE", source, destination, client.codec().encode(member)))
                == 1;
    }

    public Object sscan(String key, long cursor, Object... options) {
        List<Object> args = CommandArgs.args("SSCAN", key, cursor);
        args.addAll(List.of(options));
        return client.command(args);
    }

    private List<Object> members(String command, String... keys) {
        HashSlot.requireSame(command, List.of(keys));
        List<Object> args = CommandArgs.args(command);
        args.addAll(List.of(keys));
        return Resp.list(client.command(args)).stream().map(this::decode).toList();
    }

    private long store(String command, String destination, String... keys) {
        List<String> all = new java.util.ArrayList<>();
        all.add(destination);
        all.addAll(List.of(keys));
        HashSlot.requireSame(command, all);
        List<Object> args = CommandArgs.args(command, destination);
        args.addAll(List.of(keys));
        return Resp.number(client.command(args));
    }

    private Object decode(Object value) {
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }
}
