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

    public boolean pfmerge(String destination, String... sourceKeys) {
        List<String> all = new java.util.ArrayList<>();
        all.add(destination);
        all.addAll(List.of(sourceKeys));
        HashSlot.requireSame("PFMERGE", all);
        List<Object> args = CommandArgs.args("PFMERGE", destination);
        args.addAll(List.of(sourceKeys));
        return CommandArgs.ok(client.command(args));
    }
}
