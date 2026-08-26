package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Compact request codec for semantics-preserving many-item Flow mutations. */
final class NativeFlowManyCodec {
    private static final int COMPLETE_MANY = 0x92;
    private static final int COMPLETE_MANY_OK = 0x93;
    private static final int TRANSITION_MANY = 0x9B;
    private static final int TRANSITION_MANY_OK = 0x9C;
    private static final int MAX_BYTES = 64 * 1024 * 1024;
    private static final String MALFORMED_UTF8 = "compact Flow strings must contain valid UTF-8";
    private static final String TOO_LARGE =
            "compact Flow many request exceeds the maximum request size";
    private static final Object INVALID_BINARY = new Object();
    private static final Set<String> COMPLETE_FIELDS =
            Set.of("partition_key", "now_ms", "independent", "items", "return");
    private static final Set<String> TRANSITION_FIELDS =
            Set.of(
                    "partition_key",
                    "from_state",
                    "to_state",
                    "now_ms",
                    "run_at_ms",
                    "independent",
                    "items",
                    "return");

    private NativeFlowManyCodec() {}

    static Encoded tryEncode(FlowManyCommandEncoder.Prepared prepared) {
        if (prepared == null) {
            return null;
        }
        return switch (prepared.command()) {
            case COMPLETE_MANY -> completeMany(prepared.payload());
            case TRANSITION_MANY -> transitionMany(prepared.payload());
            default -> null;
        };
    }

    private static Encoded completeMany(Map<String, Object> payload) {
        if (!COMPLETE_FIELDS.containsAll(payload.keySet())) {
            return null;
        }
        Long nowMs = integer(payload.get("now_ms"));
        List<?> items = list(payload.get("items"));
        Object partition = optionalBinaryValue(payload.get("partition_key"));
        Integer marker = marker(payload.get("return"), COMPLETE_MANY, COMPLETE_MANY_OK);
        if (nowMs == null || items == null || marker == null || partition == INVALID_BINARY) {
            return null;
        }
        long size = 1L + optionalBinarySize(partition) + Long.BYTES + 1 + Integer.BYTES;
        CompleteItem[] encodedItems = new CompleteItem[items.size()];
        for (int index = 0; index < items.size(); index++) {
            CompleteItem item = completeItem(items.get(index));
            if (item == null) {
                return null;
            }
            encodedItems[index] = item;
            size = add(size, binarySize(item.id()));
            size = add(size, optionalBinarySize(item.partition()));
            size = add(size, binarySize(item.lease()));
            size = add(size, Long.BYTES);
        }
        byte[] result = allocate(size);
        int offset = 0;
        result[offset++] = marker.byteValue();
        offset = writeOptionalBinary(result, offset, partition);
        offset = writeLong(result, offset, nowMs);
        result[offset++] = booleanMarker(payload.get("independent"));
        offset = writeInt(result, offset, encodedItems.length);
        for (CompleteItem item : encodedItems) {
            offset = writeBinary(result, offset, item.id());
            offset = writeOptionalBinary(result, offset, item.partition());
            offset = writeBinary(result, offset, item.lease());
            offset = writeLong(result, offset, item.fencing());
        }
        return new Encoded(result);
    }

    private static Encoded transitionMany(Map<String, Object> payload) {
        if (!TRANSITION_FIELDS.containsAll(payload.keySet())) {
            return null;
        }
        Object fromState = binaryValue(payload.get("from_state"));
        Object toState = binaryValue(payload.get("to_state"));
        Object partition = optionalBinaryValue(payload.get("partition_key"));
        Long nowMs = integer(payload.get("now_ms"));
        Long runAtMs = integer(payload.get("run_at_ms"));
        List<?> items = list(payload.get("items"));
        Integer marker = marker(payload.get("return"), TRANSITION_MANY, TRANSITION_MANY_OK);
        if (runAtMs == null) {
            runAtMs = nowMs;
        }
        if (fromState == null
                || toState == null
                || partition == INVALID_BINARY
                || nowMs == null
                || runAtMs == null
                || items == null
                || marker == null) {
            return null;
        }
        long size = 1L + binarySize(fromState) + binarySize(toState);
        size = add(size, optionalBinarySize(partition) + Long.BYTES * 2L + 1 + Integer.BYTES);
        TransitionItem[] encodedItems = new TransitionItem[items.size()];
        for (int index = 0; index < items.size(); index++) {
            TransitionItem item = transitionItem(items.get(index));
            if (item == null) {
                return null;
            }
            encodedItems[index] = item;
            size = add(size, binarySize(item.id()));
            size = add(size, optionalBinarySize(item.partition()));
            size = add(size, Long.BYTES);
            size = add(size, optionalBinarySize(item.lease()));
        }
        byte[] result = allocate(size);
        int offset = 0;
        result[offset++] = marker.byteValue();
        offset = writeBinary(result, offset, fromState);
        offset = writeBinary(result, offset, toState);
        offset = writeOptionalBinary(result, offset, partition);
        offset = writeLong(result, offset, nowMs);
        offset = writeLong(result, offset, runAtMs);
        result[offset++] = booleanMarker(payload.get("independent"));
        offset = writeInt(result, offset, encodedItems.length);
        for (TransitionItem item : encodedItems) {
            offset = writeBinary(result, offset, item.id());
            offset = writeOptionalBinary(result, offset, item.partition());
            offset = writeLong(result, offset, item.fencing());
            offset = writeOptionalBinary(result, offset, item.lease());
        }
        return new Encoded(result);
    }

    private static CompleteItem completeItem(Object value) {
        if (!(value instanceof List<?> item) || (item.size() != 3 && item.size() != 4)) {
            return null;
        }
        boolean mixed = item.size() == 4;
        Object id = binaryValue(item.get(0));
        Object partition = mixed ? binaryValue(item.get(1)) : null;
        Object lease = binaryValue(item.get(mixed ? 2 : 1));
        Long fencing = integer(item.get(mixed ? 3 : 2));
        return id == null || (mixed && partition == null) || lease == null || fencing == null
                ? null
                : new CompleteItem(id, partition, lease, fencing);
    }

    private static TransitionItem transitionItem(Object value) {
        if (!(value instanceof Map<?, ?> item)) {
            return null;
        }
        Object id = binaryValue(mapValue(item, "id"));
        Object partition = optionalBinaryValue(mapValue(item, "partition_key"));
        Object lease = optionalBinaryValue(mapValue(item, "lease_token"));
        Long fencing = integer(mapValue(item, "fencing_token"));
        return id == null
                        || partition == INVALID_BINARY
                        || lease == INVALID_BINARY
                        || fencing == null
                ? null
                : new TransitionItem(id, partition, fencing, lease);
    }

    private static Object mapValue(Map<?, ?> map, String key) {
        return map.get(key);
    }

    private static Integer marker(Object value, int ordinary, int ok) {
        if (value == null) {
            return ordinary;
        }
        String text = text(value);
        return text != null && "OK_ON_SUCCESS".equalsIgnoreCase(text) ? ok : null;
    }

    private static byte booleanMarker(Object value) {
        if (value == null) {
            return 0;
        }
        return (byte) (Boolean.TRUE.equals(value) ? 2 : 1);
    }

    private static List<?> list(Object value) {
        return value instanceof List<?> list ? list : null;
    }

    private static Long integer(Object value) {
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private static Object optionalBinaryValue(Object value) {
        if (value == null) {
            return null;
        }
        Object encoded = binaryValue(value);
        return encoded == null ? INVALID_BINARY : encoded;
    }

    private static Object binaryValue(Object value) {
        if (value instanceof byte[]) {
            return value;
        }
        if (!(value instanceof String text)) {
            return null;
        }
        binaryLength(text);
        return text;
    }

    private static String text(Object value) {
        if (value instanceof String text) {
            return text;
        }
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : null;
    }

    private static int binaryLength(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        return NativeUtf8.length((String) value, MAX_BYTES, MALFORMED_UTF8, TOO_LARGE);
    }

    private static NativeProtocolException tooLarge() {
        return new NativeProtocolException(TOO_LARGE);
    }

    private static long binarySize(Object value) {
        return Integer.BYTES + (long) binaryLength(value);
    }

    private static long optionalBinarySize(Object value) {
        return value == null ? Integer.BYTES : binarySize(value);
    }

    private static long add(long current, long bytes) {
        long result = current + bytes;
        if (result > MAX_BYTES) {
            throw tooLarge();
        }
        return result;
    }

    private static byte[] allocate(long size) {
        if (size > MAX_BYTES) {
            throw tooLarge();
        }
        return new byte[(int) size];
    }

    private static int writeBinary(byte[] target, int offset, Object value) {
        int length = binaryLength(value);
        int data = writeInt(target, offset, length);
        if (value instanceof byte[] bytes) {
            System.arraycopy(bytes, 0, target, data, bytes.length);
        } else {
            NativeUtf8.write(target, data, (String) value);
        }
        return data + length;
    }

    private static int writeOptionalBinary(byte[] target, int offset, Object value) {
        return value == null ? writeInt(target, offset, -1) : writeBinary(target, offset, value);
    }

    private static int writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
        return offset + Integer.BYTES;
    }

    private static int writeLong(byte[] target, int offset, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            target[offset++] = (byte) (value >>> shift);
        }
        return offset;
    }

    record Encoded(byte[] payload) {}

    private record CompleteItem(Object id, Object partition, Object lease, long fencing) {}

    private record TransitionItem(Object id, Object partition, long fencing, Object lease) {}
}
