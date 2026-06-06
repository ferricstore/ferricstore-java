package com.ferricstore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class RawCodec implements Codec {
    @Override
    public byte[] encode(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.slice();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object decode(byte[] value) {
        return value;
    }

    @Override
    public <T> T decode(byte[] value, Class<T> type) {
        if (value == null) {
            return null;
        }
        if (type == byte[].class || type == Object.class) {
            return type.cast(value);
        }
        if (type == String.class) {
            return type.cast(new String(value, StandardCharsets.UTF_8));
        }
        throw new FerricStoreException("RawCodec cannot decode " + type.getName());
    }
}
