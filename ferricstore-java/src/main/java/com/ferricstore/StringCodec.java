package com.ferricstore;

import java.nio.charset.StandardCharsets;

public final class StringCodec implements Codec {
    @Override
    public byte[] encode(Object value) {
        return value == null ? null : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object decode(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public <T> T decode(byte[] value, Class<T> type) {
        Object decoded = decode(value);
        return decoded == null ? null : type.cast(decoded);
    }
}
