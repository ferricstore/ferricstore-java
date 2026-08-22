package com.ferricstore;

import java.util.List;

public final class SortedSetStore {
    private final FerricStoreClient client;

    SortedSetStore(FerricStoreClient client) {
        this.client = client;
    }

    public long zadd(String key, List<ZAddMember> members) {
        return Resp.number(zadd(key, members, ZAddOptions.builder().build()));
    }

    public Object zadd(String key, List<ZAddMember> members, ZAddOptions options) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("ZADD requires at least one score/member pair");
        }
        java.util.Objects.requireNonNull(options, "ZADD options");
        if (options.incr() && members.size() != 1) {
            throw new IllegalArgumentException("ZADD INCR requires exactly one score/member pair");
        }
        List<Object> args = CommandArgs.args("ZADD", key);
        appendFlag(args, "NX", options.nx());
        appendFlag(args, "XX", options.xx());
        appendFlag(args, "GT", options.gt());
        appendFlag(args, "LT", options.lt());
        appendFlag(args, "CH", options.ch());
        appendFlag(args, "INCR", options.incr());
        for (ZAddMember member : members) {
            args.add(member.score());
            args.add(client.codec().encode(member.member()));
        }
        return client.command(args);
    }

    public String zscore(String key, Object member) {
        Object response = client.command("ZSCORE", key, client.codec().encode(member));
        return response == null ? null : Resp.string(response);
    }

    public List<Object> zrange(String key, long start, long stop) {
        return decodedList(client.command("ZRANGE", key, start, stop));
    }

    public List<Object> zrange(String key, long start, long stop, Object... options) {
        return range("ZRANGE", key, start, stop, options);
    }

    public List<Object> zrevrange(String key, long start, long stop, Object... options) {
        return range("ZREVRANGE", key, start, stop, options);
    }

    public long zrem(String key, Object... members) {
        List<Object> args = CommandArgs.args("ZREM", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.number(client.command(args));
    }

    public long zcard(String key) {
        return Resp.number(client.command("ZCARD", key));
    }

    public double zincrBy(String key, double amount, Object member) {
        return Double.parseDouble(
                Resp.string(client.command("ZINCRBY", key, amount, client.codec().encode(member))));
    }

    public long zcount(String key, Object min, Object max) {
        return Resp.number(client.command("ZCOUNT", key, min, max));
    }

    public Long zrank(String key, Object member) {
        return optionalNumber(client.command("ZRANK", key, client.codec().encode(member)));
    }

    public Long zrevrank(String key, Object member) {
        return optionalNumber(client.command("ZREVRANK", key, client.codec().encode(member)));
    }

    public List<String> zmscore(String key, Object... members) {
        List<Object> args = CommandArgs.args("ZMSCORE", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.list(client.command(args)).stream()
                .map(value -> value == null ? null : Resp.string(value))
                .toList();
    }

    public List<Object> zpopmin(String key, Integer count) {
        return pop("ZPOPMIN", key, count);
    }

    public List<Object> zpopmax(String key, Integer count) {
        return pop("ZPOPMAX", key, count);
    }

    public Object zrandmember(String key, Object... options) {
        List<Object> args = CommandArgs.args("ZRANDMEMBER", key);
        args.addAll(List.of(options));
        Object value = client.command(args);
        return options.length == 0 ? decode(value) : decodedList(value);
    }

    public Object zscan(String key, long cursor, Object... options) {
        List<Object> args = CommandArgs.args("ZSCAN", key, cursor);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public List<Object> zrangeByScore(String key, Object min, Object max, Object... options) {
        return range("ZRANGEBYSCORE", key, min, max, options);
    }

    public List<Object> zrevrangeByScore(String key, Object max, Object min, Object... options) {
        return range("ZREVRANGEBYSCORE", key, max, min, options);
    }

    private List<Object> range(
            String command, String key, Object start, Object stop, Object... options) {
        List<Object> args = CommandArgs.args(command, key, start, stop);
        args.addAll(List.of(options));
        return decodedList(client.command(args));
    }

    private List<Object> pop(String command, String key, Integer count) {
        Object value =
                count == null ? client.command(command, key) : client.command(command, key, count);
        return decodedList(value);
    }

    private List<Object> decodedList(Object value) {
        return Resp.list(value).stream().map(this::decode).toList();
    }

    private Object decode(Object value) {
        return value instanceof byte[] bytes ? client.codec().decode(bytes) : value;
    }

    private static Long optionalNumber(Object value) {
        return value == null ? null : Resp.number(value);
    }

    private static void appendFlag(List<Object> args, String flag, boolean enabled) {
        if (enabled) {
            args.add(flag);
        }
    }
}
