package com.ferricstore;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Shared semantics-preserving parser for homogeneous FLOW.CREATE pipelines. */
final class FlowCreatePipeline {
    private FlowCreatePipeline() {}

    static Batch tryParse(List<List<Object>> commands) {
        if (commands == null || commands.isEmpty()) {
            return null;
        }
        List<CreateItem> items = new ArrayList<>(commands.size());
        Metadata metadata = null;
        for (List<Object> command : commands) {
            ParsedCreate parsed = parse(command, metadata);
            if (parsed == null) {
                return null;
            }
            if (metadata == null) {
                metadata = parsed.metadata();
            }
            items.add(parsed.item());
        }
        return new Batch(metadata, List.copyOf(items));
    }

    @SuppressWarnings("PMD.NcssCount") // One exhaustive option switch defines the compact shape.
    private static ParsedCreate parse(List<Object> command, Metadata expected) {
        if (command == null || command.size() < 2 || !"FLOW.CREATE".equals(token(command.get(0)))) {
            return null;
        }
        byte[] id = binary(command.get(1));
        if (id == null) {
            return null;
        }
        byte[] type = null;
        byte[] state = null;
        byte[] payload = null;
        Long nowMs = null;
        Long runAtMs = null;
        boolean payloadSeen = false;
        boolean prioritySeen = false;
        boolean typeSeen = false;
        boolean stateSeen = false;
        for (int index = 2; index < command.size(); index += 2) {
            if (index + 1 >= command.size()) {
                return null;
            }
            String option = token(command.get(index));
            Object value = command.get(index + 1);
            if (option == null) {
                return null;
            }
            switch (option) {
                case "TYPE" -> {
                    if (typeSeen) {
                        return null;
                    }
                    type =
                            expected == null
                                    ? binary(value)
                                    : matchingBinary(value, expected.type());
                    if (type == null) {
                        return null;
                    }
                    typeSeen = true;
                }
                case "STATE" -> {
                    if (stateSeen) {
                        return null;
                    }
                    state =
                            expected == null
                                    ? binary(value)
                                    : matchingBinary(value, expected.state());
                    if (state == null) {
                        return null;
                    }
                    stateSeen = true;
                }
                case "NOW" -> {
                    Long encoded = integer(value);
                    if (nowMs != null || encoded == null) {
                        return null;
                    }
                    nowMs = encoded;
                }
                case "RUN_AT" -> {
                    Long encoded = integer(value);
                    if (runAtMs != null || encoded == null) {
                        return null;
                    }
                    runAtMs = encoded;
                }
                case "PAYLOAD" -> {
                    byte[] encoded = binary(value);
                    if (payloadSeen || encoded == null) {
                        return null;
                    }
                    payload = encoded;
                    payloadSeen = true;
                }
                case "PRIORITY" -> {
                    Long priority = integer(value);
                    if (prioritySeen || priority == null || priority != 0) {
                        return null;
                    }
                    prioritySeen = true;
                }
                default -> {
                    return null;
                }
            }
        }
        if (type == null || state == null || nowMs == null || runAtMs == null) {
            return null;
        }
        if (expected != null && (expected.nowMs() != nowMs || expected.runAtMs() != runAtMs)) {
            return null;
        }
        return new ParsedCreate(
                expected == null ? new Metadata(type, state, nowMs, runAtMs) : expected,
                new CreateItem(id, payload, payloadSeen));
    }

    private static String token(Object value) {
        if (value instanceof String text) {
            return text.toUpperCase(Locale.ROOT);
        }
        if (!(value instanceof byte[] bytes)) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString()
                    .toUpperCase(Locale.ROOT);
        } catch (CharacterCodingException ignored) {
            return null;
        }
    }

    private static byte[] binary(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (!(value instanceof String text) || !validUtf16(text)) {
            return null;
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] matchingBinary(Object value, byte[] expected) {
        if (value instanceof byte[] bytes) {
            return Arrays.equals(bytes, expected) ? expected : null;
        }
        return value instanceof String text && utf8Equals(expected, text) ? expected : null;
    }

    private static boolean utf8Equals(byte[] expected, String value) {
        int offset = 0;
        int index = 0;
        while (index < value.length()) {
            char character = value.charAt(index++);
            if (character < 0x80) {
                if (!matches(expected, offset++, character)) {
                    return false;
                }
            } else if (character < 0x800) {
                if (!matches(expected, offset++, 0xc0 | character >> 6)
                        || !matches(expected, offset++, 0x80 | character & 0x3f)) {
                    return false;
                }
            } else if (Character.isHighSurrogate(character)) {
                if (index >= value.length() || !Character.isLowSurrogate(value.charAt(index))) {
                    return false;
                }
                int codePoint = Character.toCodePoint(character, value.charAt(index++));
                if (!matches(expected, offset++, 0xf0 | codePoint >> 18)
                        || !matches(expected, offset++, 0x80 | codePoint >> 12 & 0x3f)
                        || !matches(expected, offset++, 0x80 | codePoint >> 6 & 0x3f)
                        || !matches(expected, offset++, 0x80 | codePoint & 0x3f)) {
                    return false;
                }
            } else if (Character.isLowSurrogate(character)
                    || !matches(expected, offset++, 0xe0 | character >> 12)
                    || !matches(expected, offset++, 0x80 | character >> 6 & 0x3f)
                    || !matches(expected, offset++, 0x80 | character & 0x3f)) {
                return false;
            }
        }
        return offset == expected.length;
    }

    private static boolean matches(byte[] expected, int offset, int value) {
        return offset < expected.length && expected[offset] == (byte) value;
    }

    private static boolean validUtf16(String value) {
        int index = 0;
        while (index < value.length()) {
            char character = value.charAt(index);
            if (Character.isHighSurrogate(character)) {
                index++;
                if (index >= value.length() || !Character.isLowSurrogate(value.charAt(index))) {
                    return false;
                }
            } else if (Character.isLowSurrogate(character)) {
                return false;
            }
            index++;
        }
        return true;
    }

    private static Long integer(Object value) {
        return value instanceof Byte
                        || value instanceof Short
                        || value instanceof Integer
                        || value instanceof Long
                ? ((Number) value).longValue()
                : null;
    }

    record Batch(Metadata metadata, List<CreateItem> items) {
        int count() {
            return items.size();
        }

        boolean hasPayloadForEveryItem() {
            return items.stream().allMatch(CreateItem::payloadPresent);
        }

        @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
        Map<String, Object> typedPayload() {
            List<Map<String, Object>> encodedItems = new ArrayList<>(items.size());
            for (CreateItem item : items) {
                Map<String, Object> encoded = new LinkedHashMap<>();
                encoded.put("id", item.id());
                if (item.payloadPresent()) {
                    encoded.put("payload", item.payload());
                }
                encodedItems.add(encoded);
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("items", encodedItems);
            payload.put("type", metadata.type());
            payload.put("state", metadata.state());
            payload.put("now_ms", metadata.nowMs());
            payload.put("run_at_ms", metadata.runAtMs());
            payload.put("priority", 0L);
            payload.put("independent", true);
            payload.put("return", "ok_on_success");
            return payload;
        }
    }

    record CreateItem(byte[] id, byte[] payload, boolean payloadPresent) {}

    private record ParsedCreate(Metadata metadata, CreateItem item) {}

    record Metadata(byte[] type, byte[] state, long nowMs, long runAtMs) {}
}
