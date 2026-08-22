package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Resp {
    private Resp() {}

    static List<FlowRecord> records(Object value, Codec codec) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new FerricStoreException(
                    "expected RESP array, got " + value.getClass().getSimpleName());
        }
        return list.stream().map(item -> record(item, codec)).toList();
    }

    static FlowRecord optionalRecord(Object value, Codec codec) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list && list.isEmpty()) {
            return null;
        }
        if (value instanceof Map<?, ?> map && map.isEmpty()) {
            return null;
        }
        return record(value, codec);
    }

    static FlowRecord record(Object value, Codec codec) {
        Map<String, Object> map = map(value);
        Object error = decodeStructured(codec, map.get("error"));
        Map<String, Object> errorFields = stringObjectMap(error);
        String failureReason = optionalString(errorFields.get("reason"));
        Long maxActiveMs = positiveLong(map.get("max_active_ms"));
        if (maxActiveMs == null) {
            maxActiveMs = positiveLong(errorFields.get("max_active_ms"));
        }
        if ("max_active_ms".equals(failureReason) && maxActiveMs == null) {
            throw new FerricStoreException(
                    "FLOW max_active_ms failure response is missing a positive max_active_ms");
        }
        return new FlowRecord(
                string(map.get("id")),
                string(map.get("type")),
                string(map.get("state")),
                optionalString(map.get("partition_key")),
                decode(codec, map.get("payload")),
                error,
                failureReason,
                maxActiveMs,
                optionalString(map.get("lease_token")),
                number(map.get("fencing_token")),
                number(map.get("version")),
                optionalString(map.get("parent_flow_id")),
                optionalString(map.get("root_flow_id")),
                optionalString(map.get("correlation_id")),
                decodeValueMap(codec, map.get("values")),
                stringObjectMap(map.get("value_refs")),
                stringObjectMap(map.get("attributes")),
                stringObjectMap(map.get("state_meta")),
                map);
    }

    private static Object decodeStructured(Codec codec, Object value) {
        return value instanceof byte[] bytes ? codec.decode(bytes) : normalize(value);
    }

    private static Long positiveLong(Object value) {
        if (value == null) {
            return null;
        }
        long parsed;
        try {
            parsed =
                    value instanceof Number number
                            ? number.longValue()
                            : Long.parseLong(string(value));
        } catch (NumberFormatException ignored) {
            return null;
        }
        return parsed > 0 ? parsed : null;
    }

    static List<ClaimedItem> claimedItems(Object value) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new FerricStoreException(
                    "expected RESP array, got " + value.getClass().getSimpleName());
        }
        return list.stream().map(Resp::claimedItem).toList();
    }

    static ClaimedItem claimedItem(Object value) {
        if (value instanceof List<?> list) {
            if (list.size() < 4 || list.size() > 6) {
                throw new FerricStoreException("expected claimed item array with 4 to 6 fields");
            }
            Map<String, Object> legacyAttributes =
                    list.size() > 4 && mapLike(list.get(4))
                            ? stringObjectMap(list.get(4))
                            : Map.of();
            Map<String, Object> attributes =
                    list.size() > 5 ? stringObjectMap(list.get(5)) : legacyAttributes;
            return new ClaimedItem(
                    string(list.get(0)),
                    string(list.get(2)),
                    number(list.get(3)),
                    optionalString(list.get(1)),
                    "",
                    "running",
                    list.size() > 4 && legacyAttributes.isEmpty()
                            ? optionalString(list.get(4))
                            : null,
                    null,
                    attributes);
        }
        Map<String, Object> map = map(value);
        return new ClaimedItem(
                string(map.get("id")),
                string(map.get("lease_token")),
                number(map.get("fencing_token")),
                optionalString(map.get("partition_key")),
                string(map.get("type")),
                optionalString(map.get("state")) == null
                        ? "running"
                        : optionalString(map.get("state")),
                optionalString(map.get("run_state")),
                map.get("payload"),
                stringObjectMap(map.get("attributes")));
    }

    private static boolean mapLike(Object value) {
        return value instanceof Map<?, ?>
                || value instanceof List<?> list
                        && ((!list.isEmpty() && isPairList(list)) || list.size() % 2 == 0);
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
            if (isPairList(list)) {
                Map<String, Object> mapped = new LinkedHashMap<>();
                for (Object pair : list) {
                    mapped.put(string(pairKey(pair)), normalize(pairValue(pair)));
                }
                return mapped;
            }
            if (list.size() % 2 != 0) {
                throw new FerricStoreException("expected RESP map-like array with even length");
            }
            Map<String, Object> mapped = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                mapped.put(string(list.get(i)), normalize(list.get(i + 1)));
            }
            return mapped;
        }
        throw new FerricStoreException(
                "expected RESP map, got " + value.getClass().getSimpleName());
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

    static List<Map<String, Object>> maps(Object value) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new FerricStoreException(
                    "expected RESP array, got " + value.getClass().getSimpleName());
        }
        return list.stream().map(Resp::map).toList();
    }

    static Map<String, Object> optionalMap(Object value) {
        if (value == null
                || value instanceof List<?> list && list.isEmpty()
                || value instanceof Map<?, ?> map && map.isEmpty()) {
            return null;
        }
        return map(value);
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
        raw.forEach(
                (name, item) ->
                        decoded.put(
                                name, item instanceof byte[] bytes ? codec.decode(bytes) : item));
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
        if (value instanceof List<?> list) {
            if (isPairList(list)) {
                Map<String, Object> result = new LinkedHashMap<>();
                for (Object pair : list) {
                    result.put(string(pairKey(pair)), normalize(pairValue(pair)));
                }
                return result;
            }
            if (list.size() % 2 != 0) {
                return Map.of();
            }
            Map<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                result.put(string(list.get(i)), normalize(list.get(i + 1)));
            }
            return result;
        }
        return Map.of();
    }

    private static boolean isPairList(List<?> list) {
        return !list.isEmpty() && list.stream().allMatch(Resp::isPair);
    }

    private static boolean isPair(Object value) {
        return value instanceof Map.Entry<?, ?>
                || value instanceof List<?> list && list.size() == 2;
    }

    private static Object pairKey(Object value) {
        if (value instanceof Map.Entry<?, ?> entry) {
            return entry.getKey();
        }
        return ((List<?>) value).get(0);
    }

    private static Object pairValue(Object value) {
        if (value instanceof Map.Entry<?, ?> entry) {
            return entry.getValue();
        }
        return ((List<?>) value).get(1);
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
