package com.ferricstore;

import java.util.List;
import java.util.Map;

public final class StreamStore {
    private final FerricStoreClient client;

    StreamStore(FerricStoreClient client) {
        this.client = client;
    }

    public Object xadd(String key, String id, Map<String, ?> fields) {
        List<Object> args = CommandArgs.args("XADD", key, id);
        fields.forEach(
                (field, value) -> {
                    args.add(field);
                    args.add(client.codec().encode(value));
                });
        return client.command(args);
    }

    public long xlen(String key) {
        return Resp.number(client.command("XLEN", key));
    }

    public List<Object> xrange(String key, String start, String end) {
        return Resp.list(client.command("XRANGE", key, start, end));
    }

    public long xack(String key, String group, String... ids) {
        List<Object> args = CommandArgs.args("XACK", key, group);
        args.addAll(List.of(ids));
        return Resp.number(client.command(args));
    }
}
