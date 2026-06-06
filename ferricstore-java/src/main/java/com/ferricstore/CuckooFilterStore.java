package com.ferricstore;

public final class CuckooFilterStore {
    private final FerricStoreClient client;

    CuckooFilterStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean reserve(String key, long capacity) { return CommandArgs.ok(client.command("CF.RESERVE", key, capacity)); }
    public boolean add(String key, Object element) { return Resp.number(client.command("CF.ADD", key, client.codec().encode(element))) == 1; }
    public boolean addnx(String key, Object element) { return Resp.number(client.command("CF.ADDNX", key, client.codec().encode(element))) == 1; }
    public boolean del(String key, Object element) { return Resp.number(client.command("CF.DEL", key, client.codec().encode(element))) == 1; }
    public boolean exists(String key, Object element) { return Resp.number(client.command("CF.EXISTS", key, client.codec().encode(element))) == 1; }
    public long count(String key, Object element) { return Resp.number(client.command("CF.COUNT", key, client.codec().encode(element))); }
    public Object info(String key) { return client.command("CF.INFO", key); }
}
