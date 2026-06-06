package com.ferricstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public final class JsonStore {
    private final FerricStoreClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    JsonStore(FerricStoreClient client) {
        this.client = client;
    }

    public boolean set(String key, String path, Object value) {
        return CommandArgs.ok(client.command("JSON.SET", key, path, write(value)));
    }

    public <T> T get(String key, Class<T> type) {
        Object response = client.command("JSON.GET", key, "$");
        if (response == null) {
            return null;
        }
        try {
            return mapper.readValue(Resp.string(response), type);
        } catch (Exception e) {
            throw new FerricStoreException("failed to decode JSON.GET response", e);
        }
    }

    public long del(String key, String path) { return Resp.number(client.command("JSON.DEL", key, path)); }
    public List<Object> mget(List<String> keys, String path) {
        List<Object> args = CommandArgs.args("JSON.MGET");
        args.addAll(keys);
        args.add(path);
        return Resp.list(client.command(args));
    }

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new FerricStoreException("failed to encode JSON command value", e);
        }
    }
}
