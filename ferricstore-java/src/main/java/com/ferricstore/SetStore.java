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
}
