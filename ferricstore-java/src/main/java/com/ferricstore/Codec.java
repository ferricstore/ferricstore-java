package com.ferricstore;

public interface Codec {
    byte[] encode(Object value);

    Object decode(byte[] value);

    <T> T decode(byte[] value, Class<T> type);
}
