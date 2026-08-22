package com.ferricstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public final class JsonStore {
    private final FerricStoreClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    JsonStore(FerricStoreClient client) {
        this.client = client;
    }

    /** Stores a complete JSON document using FerricStore's supported string command surface. */
    public boolean set(String key, String path, Object value) {
        requireRoot(path);
        return CommandArgs.ok(client.command("SET", key, write(value)));
    }

    public <T> T get(String key, Class<T> type) {
        Object response = client.command("GET", key);
        if (response == null) {
            return null;
        }
        try {
            return mapper.readValue(Resp.string(response), type);
        } catch (Exception e) {
            throw new FerricStoreException("failed to decode JSON.GET response", e);
        }
    }

    public long del(String key, String path) {
        requireRoot(path);
        return Resp.number(client.command("DEL", key));
    }

    public List<Object> mget(List<String> keys, String path) {
        requireRoot(path);
        List<Object> args = CommandArgs.args("MGET");
        args.addAll(keys);
        return Resp.list(client.command(args));
    }

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new FerricStoreException("failed to encode JSON command value", e);
        }
    }

    private static void requireRoot(String path) {
        if (!"$".equals(path)) {
            throw new IllegalArgumentException(
                    "FerricStore JSON documents support the root '$' path only");
        }
    }
}
