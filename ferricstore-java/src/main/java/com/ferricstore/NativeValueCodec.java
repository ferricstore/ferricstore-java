package com.ferricstore;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class NativeValueCodec {
    private static final int MAX_NESTING = 128;
    private static final int MAX_COLLECTION_ITEMS = 1_000_000;

    private NativeValueCodec() {}

    static byte[] encode(Object value, int maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be non-negative");
        }
        BoundedBuffer buffer = new BoundedBuffer(maxBytes);
        try {
            writeValue(new DataOutputStream(buffer), value, 0, buffer);
            return buffer.toByteArray();
        } catch (IOException error) {
            throw new NativeProtocolException("failed to encode native protocol value", error);
        }
    }

    static Object decode(byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            throw new NativeProtocolException("native protocol value is empty");
        }
        return decode(encoded, 0);
    }

    static Object decode(byte[] encoded, int offset) {
        if (encoded == null || offset < 0 || offset >= encoded.length) {
            throw new NativeProtocolException("native protocol value is empty");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        input.position(offset);
        Object value = readValue(input, 0, new DecodeBudget(MAX_COLLECTION_ITEMS));
        if (input.hasRemaining()) {
            throw new NativeProtocolException("native protocol value has trailing bytes");
        }
        return value;
    }

    private static void writeValue(
            DataOutputStream output, Object value, int depth, BoundedBuffer buffer)
            throws IOException {
        checkDepth(depth);
        if (value == null) {
            output.writeByte(0);
        } else if (Boolean.TRUE.equals(value)) {
            output.writeByte(1);
        } else if (Boolean.FALSE.equals(value)) {
            output.writeByte(2);
        } else if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            output.writeByte(3);
            output.writeLong(((Number) value).longValue());
        } else if (value instanceof byte[] bytes) {
            writeBinary(output, bytes, buffer);
        } else if (value instanceof String text) {
            writeBinary(output, utf8(text, "native protocol strings"), buffer);
        } else if (value instanceof List<?> list) {
            writeCount(output, 5, list.size(), buffer);
            for (Object item : list) {
                writeValue(output, item, depth + 1, buffer);
            }
        } else if (value instanceof Object[] values) {
            writeCount(output, 5, values.length, buffer);
            for (Object item : values) {
                writeValue(output, item, depth + 1, buffer);
            }
        } else if (value instanceof Map<?, ?> map) {
            writeCount(output, 6, map.size(), buffer);
            Set<ByteBuffer> keys = new HashSet<>(capacityFor(map.size()));
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                byte[] key = mapKey(entry.getKey());
                if (!keys.add(ByteBuffer.wrap(key))) {
                    throw new NativeProtocolException(
                            "duplicate native protocol map key after wire encoding");
                }
                writeLength(output, key.length, buffer);
                output.write(key);
                writeValue(output, entry.getValue(), depth + 1, buffer);
            }
        } else if (value instanceof Float || value instanceof Double) {
            output.writeByte(7);
            output.writeDouble(((Number) value).doubleValue());
        } else {
            throw new NativeProtocolException(
                    "unsupported native protocol value type: " + value.getClass().getSimpleName());
        }
    }

    private static Object readValue(ByteBuffer input, int depth, DecodeBudget budget) {
        checkDepth(depth);
        require(input, 1);
        int tag = Byte.toUnsignedInt(input.get());
        return switch (tag) {
            case 0 -> null;
            case 1 -> true;
            case 2 -> false;
            case 3 -> {
                require(input, Long.BYTES);
                yield input.getLong();
            }
            case 4 -> readBinary(input);
            case 5 -> readList(input, depth, budget);
            case 6 -> readMap(input, depth, budget);
            case 7 -> {
                require(input, Double.BYTES);
                yield input.getDouble();
            }
            default ->
                    throw new NativeProtocolException(
                            "native protocol value has unknown tag " + tag);
        };
    }

    private static List<Object> readList(ByteBuffer input, int depth, DecodeBudget budget) {
        int count = readCount(input, budget);
        List<Object> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(readValue(input, depth + 1, budget));
        }
        return result;
    }

    private static Map<String, Object> readMap(ByteBuffer input, int depth, DecodeBudget budget) {
        int count = readCount(input, budget);
        Map<String, Object> result = new LinkedHashMap<>(capacityFor(count));
        for (int index = 0; index < count; index++) {
            String key = decodeUtf8(readLengthPrefixed(input), "native protocol map keys");
            if (result.containsKey(key)) {
                throw new NativeProtocolException(
                        "duplicate native protocol map key while decoding");
            }
            result.put(key, readValue(input, depth + 1, budget));
        }
        return result;
    }

    private static int readCount(ByteBuffer input, DecodeBudget budget) {
        long count = readUnsignedInt(input);
        if (count > Integer.MAX_VALUE || count > input.remaining()) {
            throw new NativeProtocolException("native protocol collection count is invalid");
        }
        int result = (int) count;
        budget.consume(result);
        return result;
    }

    private static byte[] readBinary(ByteBuffer input) {
        return readLengthPrefixed(input);
    }

    private static byte[] readLengthPrefixed(ByteBuffer input) {
        long length = readUnsignedInt(input);
        if (length > input.remaining()) {
            throw new NativeProtocolException("native protocol value is truncated");
        }
        byte[] bytes = new byte[(int) length];
        input.get(bytes);
        return bytes;
    }

    private static long readUnsignedInt(ByteBuffer input) {
        require(input, Integer.BYTES);
        return Integer.toUnsignedLong(input.getInt());
    }

    private static void writeBinary(DataOutputStream output, byte[] bytes, BoundedBuffer buffer)
            throws IOException {
        output.writeByte(4);
        writeLength(output, bytes.length, buffer);
        output.write(bytes);
    }

    private static void writeCount(
            DataOutputStream output, int tag, int count, BoundedBuffer buffer) throws IOException {
        if (count < 0) {
            throw new NativeProtocolException("native protocol collection count is invalid");
        }
        output.writeByte(tag);
        writeLength(output, count, buffer);
    }

    private static void writeLength(DataOutputStream output, int length, BoundedBuffer buffer)
            throws IOException {
        if (length < 0 || buffer.remaining() < Integer.BYTES + (long) length) {
            throw new NativeProtocolException("encoded native protocol value exceeds maxBytes");
        }
        output.writeInt(length);
    }

    private static byte[] mapKey(Object key) {
        if (key instanceof byte[] bytes) {
            return bytes;
        }
        if (key instanceof String text) {
            return utf8(text, "native protocol map keys");
        }
        throw new NativeProtocolException(
                "native protocol map keys must be strings or byte arrays");
    }

    private static byte[] utf8(String value, String label) {
        try {
            ByteBuffer encoded =
                    StandardCharsets.UTF_8
                            .newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(java.nio.CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new NativeProtocolException(label + " must contain valid UTF-8", error);
        }
    }

    private static String decodeUtf8(byte[] value, String label) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException error) {
            throw new NativeProtocolException(label + " must contain valid UTF-8", error);
        }
    }

    private static void checkDepth(int depth) {
        if (depth > MAX_NESTING) {
            throw new NativeProtocolException(
                    "native protocol value nesting exceeds maximum depth");
        }
    }

    private static void require(ByteBuffer input, int bytes) {
        if (bytes < 0 || input.remaining() < bytes) {
            throw new NativeProtocolException("native protocol value is truncated");
        }
    }

    private static int capacityFor(int size) {
        return size < 3 ? size + 1 : Math.min(Integer.MAX_VALUE, (int) (size / 0.75f) + 1);
    }

    private static final class DecodeBudget {
        private int remaining;

        private DecodeBudget(int remaining) {
            this.remaining = remaining;
        }

        private void consume(int count) {
            if (count > remaining) {
                throw new NativeProtocolException(
                        "native protocol response exceeds collection item limit");
            }
            remaining -= count;
        }
    }

    private static final class BoundedBuffer extends ByteArrayOutputStream {
        private final int limit;

        private BoundedBuffer(int limit) {
            super(Math.min(limit, 8 * 1024));
            this.limit = limit;
        }

        private long remaining() {
            return (long) limit - count;
        }

        @Override
        public void write(int value) {
            ensure(1);
            super.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            ensure(length);
            super.write(bytes, offset, length);
        }

        private void ensure(int bytes) {
            if (bytes < 0 || bytes > remaining()) {
                throw new NativeProtocolException("encoded native protocol value exceeds maxBytes");
            }
        }
    }
}
