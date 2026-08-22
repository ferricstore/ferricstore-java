package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class NativeHelloContract {
    private static final String PREFIX =
            "FerricStore server does not satisfy the minimum 0.8.0 HELLO contract: ";

    private NativeHelloContract() {}

    static NegotiatedCapabilities parse(Object raw) {
        Map<String, Object> hello = requiredMap(raw, "HELLO response is not a map");
        if (!"ferricstore-native".equals(text(hello.get("protocol")))) {
            throw incompatible("HELLO protocol is not ferricstore-native");
        }
        if (integer(hello.get("version"), "HELLO native protocol version") != 1) {
            throw incompatible("HELLO native protocol version is not 1");
        }
        Map<String, Object> capabilities = requiredNestedMap(hello, "capabilities");
        Map<String, Object> limits = requiredNestedMap(capabilities, "limits");
        long rawLimit = integer(limits.get("max_response_bytes"), "limits.max_response_bytes");
        if (rawLimit <= 0) {
            throw incompatible("HELLO limits.max_response_bytes must be a positive integer");
        }
        int limit = (int) Math.min(rawLimit, Integer.MAX_VALUE);

        Map<String, Object> responseCodecs = requiredNestedMap(capabilities, "response_codecs");
        Map<String, Object> compactOpcodes =
                requiredNestedMap(responseCodecs, "compact_response_opcodes");
        Map<Integer, String> codecsByOpcode = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : compactOpcodes.entrySet()) {
            String codec = entry.getKey();
            if (codec.isBlank() || !(entry.getValue() instanceof List<?> opcodes)) {
                throw incompatible("compact response codec declaration is invalid");
            }
            for (Object value : opcodes) {
                long rawOpcode = integer(value, "compact response opcode");
                if (rawOpcode < 0 || rawOpcode > 0xffff) {
                    throw incompatible("compact response opcode is invalid");
                }
                int opcode = (int) rawOpcode;
                String previous = codecsByOpcode.putIfAbsent(opcode, codec);
                if (previous != null && !previous.equals(codec)) {
                    throw incompatible(
                            String.format(
                                    "opcode 0x%04x is declared by multiple compact response codecs",
                                    opcode));
                }
            }
        }
        return new NegotiatedCapabilities(
                limit, codecsByOpcode, Boolean.TRUE.equals(hello.get("auth_required")));
    }

    private static Map<String, Object> requiredNestedMap(Map<String, Object> parent, String field) {
        Object value = parent.get(field);
        if (!(value instanceof Map<?, ?>)) {
            throw incompatible("HELLO is missing " + field);
        }
        return requiredMap(value, "HELLO " + field + " is not a map");
    }

    private static Map<String, Object> requiredMap(Object value, String message) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw incompatible(message);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            String key = text(entry.getKey());
            if (key.isEmpty()) {
                throw incompatible("HELLO map contains an invalid key");
            }
            if (result.putIfAbsent(key, entry.getValue()) != null) {
                throw incompatible("HELLO map contains a duplicate key");
            }
        }
        return result;
    }

    private static long integer(Object value, String field) {
        if (!(value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long)) {
            throw incompatible("HELLO " + field + " must be an integer");
        }
        return ((Number) value).longValue();
    }

    private static String text(Object value) {
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return "";
    }

    private static NativeProtocolException incompatible(String message) {
        return new NativeProtocolException(PREFIX + message);
    }
}
