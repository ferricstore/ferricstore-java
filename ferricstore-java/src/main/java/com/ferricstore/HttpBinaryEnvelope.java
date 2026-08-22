package com.ferricstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class HttpBinaryEnvelope {
    static final String ENCODING = "ferricstore-json-v1";
    private static final String BYTES_TAG = "$ferricstore_bytes";
    private static final String MAP_TAG = "$ferricstore_map";

    private HttpBinaryEnvelope() {}

    static Object encode(Object value) {
        if (value instanceof byte[] bytes) {
            return Map.of(BYTES_TAG, Base64.getEncoder().encodeToString(bytes));
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.slice();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return Map.of(BYTES_TAG, Base64.getEncoder().encodeToString(bytes));
        }
        if (value instanceof Map<?, ?> map) {
            List<Object> pairs = new ArrayList<>(map.size());
            map.forEach(
                    (key, item) -> pairs.add(java.util.Arrays.asList(encode(key), encode(item))));
            return Map.of(MAP_TAG, pairs);
        }
        if (value instanceof List<?> list) {
            return list.stream().map(HttpBinaryEnvelope::encode).toList();
        }
        if (value instanceof Object[] array) {
            return java.util.Arrays.stream(array).map(HttpBinaryEnvelope::encode).toList();
        }
        if ((value instanceof Float floatValue && !Float.isFinite(floatValue))
                || (value instanceof Double doubleValue && !Double.isFinite(doubleValue))) {
            throw new IllegalArgumentException("HTTP command floats must be finite");
        }
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Number) {
            return value;
        }
        throw new IllegalArgumentException(
                "HTTP command value is not JSON-compatible: " + value.getClass().getSimpleName());
    }

    static Object decode(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(HttpBinaryEnvelope::decode).toList();
        }
        if (!(value instanceof Map<?, ?> map)) {
            return value;
        }
        if (map.size() == 1 && map.containsKey(BYTES_TAG)) {
            Object encoded = map.get(BYTES_TAG);
            if (!(encoded instanceof String text)) {
                throw malformed("binary marker payload must be text");
            }
            try {
                return Base64.getDecoder().decode(text);
            } catch (IllegalArgumentException error) {
                throw malformed("binary marker payload is not valid base64", error);
            }
        }
        if (map.size() == 1 && map.containsKey(MAP_TAG)) {
            Object entries = map.get(MAP_TAG);
            if (!(entries instanceof List<?> pairs)) {
                throw malformed("map marker payload must be a list");
            }
            Map<Object, Object> decoded = new LinkedHashMap<>();
            for (Object entry : pairs) {
                if (!(entry instanceof List<?> pair) || pair.size() != 2) {
                    throw malformed("map marker entries must be key/value pairs");
                }
                decoded.put(decode(pair.get(0)), decode(pair.get(1)));
            }
            return decoded;
        }
        Map<String, Object> decoded = new LinkedHashMap<>();
        map.forEach((key, item) -> decoded.put(String.valueOf(key), decode(item)));
        return decoded;
    }

    private static IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException malformed(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }
}
