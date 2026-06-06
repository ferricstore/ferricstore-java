package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Resp {
    private Resp() {
    }

    static List<FlowRecord> records(Object value, Codec codec) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new FerricStoreException("expected RESP array, got " + value.getClass().getSimpleName());
        }
        return list.stream().map(item -> record(item, codec)).toList();
    }

    static FlowRecord optionalRecord(Object value, Codec codec) {
        return value == null ? null : record(value, codec);
    }

    static FlowRecord record(Object value, Codec codec) {
        Map<String, Object> map = map(value);
        return new FlowRecord(
            string(map.get("id")),
            string(map.get("type")),
            string(map.get("state")),
            optionalString(map.get("partition_key")),
            decode(codec, map.get("payload")),
            optionalString(map.get("lease_token")),
            number(map.get("fencing_token")),
            number(map.get("version")),
            optionalString(map.get("parent_flow_id")),
            optionalString(map.get("root_flow_id")),
            optionalString(map.get("correlation_id")),
            decodeValueMap(codec, map.get("values")),
            stringObjectMap(map.get("value_refs")),
            map
        );
    }

    static Map<String, Object> map(Object value) {
        if (value instanceof Map<?, ?> raw) {
            Map<String, Object> mapped = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                mapped.put(string(entry.getKey()), normalize(entry.getValue()));
            }
            return mapped;
        }
        if (value instanceof List<?> list) {
            if (list.size() % 2 != 0) {
                throw new FerricStoreException("expected RESP map-like array with even length");
            }
            Map<String, Object> mapped = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                mapped.put(string(list.get(i)), normalize(list.get(i + 1)));
            }
            return mapped;
        }
        throw new FerricStoreException("expected RESP map, got " + value.getClass().getSimpleName());
    }

    static String string(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    static String optionalString(Object value) {
        String text = string(value);
        return text.isEmpty() ? null : text;
    }

    static byte[] bytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    static long number(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = string(value);
        return text.isEmpty() ? 0 : Long.parseLong(text);
    }

    static Object decode(Codec codec, Object value) {
        byte[] bytes = bytes(value);
        return bytes == null ? null : codec.decode(bytes);
    }

    static List<Object> list(Object value) {
        return value instanceof List<?> list ? new ArrayList<>(list) : List.of();
    }

    static Map<String, Object> parseKv(Object value) {
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            return map(value);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("value", value);
        return result;
    }

    static Map<Object, Object> testMap(Object... pairs) {
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    private static Map<String, Object> decodeValueMap(Codec codec, Object value) {
        Map<String, Object> raw = stringObjectMap(value);
        if (raw == null) {
            return Map.of();
        }
        Map<String, Object> decoded = new LinkedHashMap<>();
        raw.forEach((name, item) -> decoded.put(name, item instanceof byte[] bytes ? codec.decode(bytes) : item));
        return decoded;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> stringObjectMap(Object value) {
        if (value == null) {
            return Map.of();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(string(key), normalize(item)));
            return result;
        }
        if (value instanceof List<?> list && list.size() % 2 == 0) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                result.put(string(list.get(i)), normalize(list.get(i + 1)));
            }
            return result;
        }
        return Map.of();
    }

    private static Object normalize(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(string(key), normalize(item)));
            return result;
        }
        if (value instanceof List<?> list) {
            List<Object> normalized = new ArrayList<>(list.size());
            list.forEach(item -> normalized.add(normalize(item)));
            return normalized;
        }
        return value;
    }
}
