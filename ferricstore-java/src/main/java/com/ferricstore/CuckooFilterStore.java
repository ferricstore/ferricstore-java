package com.ferricstore;

import java.util.List;

public final class CuckooFilterStore {
    private final FerricStoreClient client;

    CuckooFilterStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean reserve(String key, long capacity, Object... options) {
        List<Object> args = CommandArgs.args("CF.RESERVE", key, capacity);
        args.addAll(List.of(options));
        return CommandArgs.ok(client.command(args));
    }

    public boolean add(String key, Object element) {
        return Resp.number(client.command("CF.ADD", key, client.codec().encode(element))) == 1;
    }

    public boolean addnx(String key, Object element) {
        return Resp.number(client.command("CF.ADDNX", key, client.codec().encode(element))) == 1;
    }

    public boolean del(String key, Object element) {
        return Resp.number(client.command("CF.DEL", key, client.codec().encode(element))) == 1;
    }

    public boolean exists(String key, Object element) {
        return Resp.number(client.command("CF.EXISTS", key, client.codec().encode(element))) == 1;
    }

    public List<Boolean> mexists(String key, Object... elements) {
        List<Object> args = CommandArgs.args("CF.MEXISTS", key);
        for (Object element : elements) {
            args.add(client.codec().encode(element));
        }
        return Resp.list(client.command(args)).stream()
                .map(value -> Resp.number(value) == 1)
                .toList();
    }

    public long count(String key, Object element) {
        return Resp.number(client.command("CF.COUNT", key, client.codec().encode(element)));
    }

    public Object info(String key) {
        return client.command("CF.INFO", key);
    }
}
