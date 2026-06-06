package com.ferricstore;

import java.util.List;

public final class HyperLogLogStore {
    private final FerricStoreClient client;

    HyperLogLogStore(FerricStoreClient client) {
        this.client = client;
    }

    public long pfadd(String key, Object... elements) {
        List<Object> args = CommandArgs.args("PFADD", key);
        for (Object element : elements) {
            args.add(client.codec().encode(element));
        }
        return Resp.number(client.command(args));
    }

    public long pfcount(String... keys) {
        List<Object> args = CommandArgs.args("PFCOUNT");
        args.addAll(List.of(keys));
        return Resp.number(client.command(args));
    }
}
