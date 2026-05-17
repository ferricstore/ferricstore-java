package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Resp {
    private Resp() {
    }

    static List<FlowRecord> records(Object value) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new FerricStoreException("expected RESP array, got " + value.getClass().getSimpleName());
        }
        return list.stream().map(Resp::record).toList();
    }

    static FlowRecord record(Object value) {
        Map<String, Object> map = map(value);
        return new FlowRecord(
            string(map.get("id")),
            string(map.get("type")),
            string(map.get("state")),
            string(map.get("partition_key")),
            bytes(map.get("payload")),
            string(map.get("lease_token")),
            number(map.get("fencing_token")),
            number(map.get("version")),
            optionalString(map.get("parent_flow_id")),
            optionalString(map.get("root_flow_id")),
            optionalString(map.get("correlation_id")),
            map
        );
    }

    static Map<String, Object> map(Object value) {
        if (value instanceof Map<?, ?> raw) {
            Map<String, Object> mapped = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                mapped.put(string(entry.getKey()), entry.getValue());
            }
            return mapped;
        }
        if (value instanceof List<?> list) {
            if (list.size() % 2 != 0) {
                throw new FerricStoreException("expected RESP map-like array with even length");
            }
            Map<String, Object> mapped = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                mapped.put(string(list.get(i)), list.get(i + 1));
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

    static Map<Object, Object> testMap(Object... pairs) {
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
