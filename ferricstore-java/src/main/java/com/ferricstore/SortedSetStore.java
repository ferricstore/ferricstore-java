package com.ferricstore;

import java.util.List;

public final class SortedSetStore {
    private final FerricStoreClient client;

    SortedSetStore(FerricStoreClient client) {
        this.client = client;
    }

    public long zadd(String key, List<ZAddMember> members) {
        List<Object> args = CommandArgs.args("ZADD", key);
        for (ZAddMember member : members) {
            args.add(member.score());
            args.add(client.codec().encode(member.member()));
        }
        return Resp.number(client.command(args));
    }

    public String zscore(String key, Object member) {
        Object response = client.command("ZSCORE", key, client.codec().encode(member));
        return response == null ? null : Resp.string(response);
    }

    public List<Object> zrange(String key, long start, long stop) {
        return Resp.list(client.command("ZRANGE", key, start, stop));
    }

    public long zrem(String key, Object... members) {
        List<Object> args = CommandArgs.args("ZREM", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.number(client.command(args));
    }
}
