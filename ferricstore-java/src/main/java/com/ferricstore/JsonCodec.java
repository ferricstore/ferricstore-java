package com.ferricstore;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonCodec implements Codec {
    private final ObjectMapper mapper;

    public JsonCodec() {
        this(new ObjectMapper());
    }

    public JsonCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] encode(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new FerricStoreException("failed to encode JSON payload", e);
        }
    }

    @Override
    public Object decode(byte[] value) {
        if (value == null) {
            return null;
        }
        try {
            return mapper.readValue(value, Object.class);
        } catch (Exception e) {
            throw new FerricStoreException("failed to decode JSON payload", e);
        }
    }

    @Override
    public <T> T decode(byte[] value, Class<T> type) {
        if (value == null) {
            return null;
        }
        try {
            return mapper.readValue(value, type);
        } catch (Exception e) {
            throw new FerricStoreException("failed to decode JSON payload as " + type.getName(), e);
        }
    }
}
