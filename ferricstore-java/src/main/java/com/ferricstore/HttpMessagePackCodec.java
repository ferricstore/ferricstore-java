package com.ferricstore;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

/** Strict binary-safe codec for FerricStore's compact HTTP command envelope. */
final class HttpMessagePackCodec {
    static final String CONTENT_TYPE = "application/vnd.ferricstore.commands+msgpack";
    static final String ENCODING = "ferricstore-msgpack-v1";
    private static final int MAX_DEPTH = 100;

    private HttpMessagePackCodec() {}

    static byte[] encode(MessageWriter writer) throws IOException {
        try (MessageBufferPacker output = MessagePack.newDefaultBufferPacker()) {
            writer.write(output);
            output.flush();
            return output.toByteArray();
        }
    }

    static void writeValue(MessagePacker output, Object value) throws IOException {
        writeValue(output, value, 0);
    }

    @SuppressWarnings("PMD.NcssCount") // One exhaustive type switch defines the wire contract.
    private static void writeValue(MessagePacker output, Object value, int depth)
            throws IOException {
        requireDepth(depth);
        if (value == null) {
            output.packNil();
        } else if (value instanceof byte[] bytes) {
            output.packBinaryHeader(bytes.length).writePayload(bytes);
        } else if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.slice();
            output.packBinaryHeader(copy.remaining());
            if (copy.hasArray()) {
                output.writePayload(
                        copy.array(), copy.arrayOffset() + copy.position(), copy.remaining());
            } else {
                byte[] bytes = new byte[copy.remaining()];
                copy.get(bytes);
                output.writePayload(bytes);
            }
        } else if (value instanceof String text) {
            output.packString(text);
        } else if (value instanceof Boolean booleanValue) {
            output.packBoolean(booleanValue);
        } else if (value instanceof Byte number) {
            output.packByte(number);
        } else if (value instanceof Short number) {
            output.packShort(number);
        } else if (value instanceof Integer number) {
            output.packInt(number);
        } else if (value instanceof Long number) {
            output.packLong(number);
        } else if (value instanceof BigInteger number) {
            output.packBigInteger(number);
        } else if (value instanceof Float number) {
            requireFinite(number);
            output.packFloat(number);
        } else if (value instanceof Double number) {
            requireFinite(number);
            output.packDouble(number);
        } else if (value instanceof List<?> list) {
            output.packArrayHeader(list.size());
            for (Object item : list) {
                writeValue(output, item, depth + 1);
            }
        } else if (value instanceof Object[] array) {
            output.packArrayHeader(array.length);
            for (Object item : array) {
                writeValue(output, item, depth + 1);
            }
        } else if (value instanceof Map<?, ?> map) {
            output.packMapHeader(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeValue(output, entry.getKey(), depth + 1);
                writeValue(output, entry.getValue(), depth + 1);
            }
        } else if (value instanceof Number) {
            throw unsupported("MessagePack numbers must be integer, float, or double values");
        } else {
            throw unsupported(
                    "HTTP command value is not MessagePack-compatible: "
                            + value.getClass().getSimpleName());
        }
    }

    static Map<String, Object> decode(byte[] body) throws IOException {
        try (MessageUnpacker input = MessagePack.newDefaultUnpacker(body)) {
            Object value = readValue(input, body.length, 0, false);
            if (input.hasNext()) {
                throw new IOException("MessagePack response contains trailing values");
            }
            if (!(value instanceof Map<?, ?> map)) {
                throw new IOException("MessagePack response root must be a map");
            }
            Map<String, Object> response = new LinkedHashMap<>(mapCapacity(map.size()));
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IOException("MessagePack response field names must be text");
                }
                response.put(key, entry.getValue());
            }
            return response;
        }
    }

    static Map<String, Object> decodeResponse(byte[] body) throws IOException {
        try (MessageUnpacker input = MessagePack.newDefaultUnpacker(body)) {
            Map<String, Object> response = readResponse(input, body.length);
            if (input.hasNext()) {
                throw new IOException("MessagePack response contains trailing values");
            }
            return response;
        }
    }

    private static Map<String, Object> readResponse(MessageUnpacker input, int bodyBytes)
            throws IOException {
        requireNext(input);
        if (input.getNextFormat().getValueType() != ValueType.MAP) {
            throw new IOException("MessagePack response root must be a map");
        }
        int count = input.unpackMapHeader();
        requireCollectionSize(count, bodyBytes / 2, "map");
        Map<String, Object> response = new LinkedHashMap<>(mapCapacity(count));
        for (int index = 0; index < count; index++) {
            Object rawKey = readValue(input, bodyBytes, 1, false);
            if (!(rawKey instanceof String key)) {
                throw new IOException("MessagePack response field names must be text");
            }
            Object value =
                    "results".equals(key)
                            ? readResults(input, bodyBytes, 1)
                            : readValue(input, bodyBytes, 1, false);
            response.put(key, value);
        }
        return response;
    }

    private static Object readResults(MessageUnpacker input, int bodyBytes, int depth)
            throws IOException {
        requireNext(input);
        if (input.getNextFormat().getValueType() != ValueType.ARRAY) {
            return readValue(input, bodyBytes, depth, false);
        }
        int count = input.unpackArrayHeader();
        requireCollectionSize(count, bodyBytes, "array");
        List<Object> results = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            results.add(readResult(input, bodyBytes, depth + 1));
        }
        return results;
    }

    private static Object readResult(MessageUnpacker input, int bodyBytes, int depth)
            throws IOException {
        requireNext(input);
        if (input.getNextFormat().getValueType() != ValueType.MAP) {
            return readValue(input, bodyBytes, depth, false);
        }
        int count = input.unpackMapHeader();
        requireCollectionSize(count, bodyBytes / 2, "map");
        Map<Object, Object> result = new LinkedHashMap<>(mapCapacity(count));
        for (int index = 0; index < count; index++) {
            Object key = readValue(input, bodyBytes, depth + 1, false);
            Object value =
                    "value".equals(key)
                            ? readValue(input, bodyBytes, depth + 1, true)
                            : readValue(input, bodyBytes, depth + 1, false);
            result.put(key, value);
        }
        return result;
    }

    private static Object readValue(
            MessageUnpacker input, int bodyBytes, int depth, boolean textAsBinary)
            throws IOException {
        if (depth > MAX_DEPTH) {
            throw new IOException("MessagePack response nesting exceeds " + MAX_DEPTH);
        }
        requireNext(input);
        ValueType type = input.getNextFormat().getValueType();
        return switch (type) {
            case NIL -> {
                input.unpackNil();
                yield null;
            }
            case BOOLEAN -> input.unpackBoolean();
            case INTEGER -> narrow(input.unpackBigInteger());
            case FLOAT -> input.unpackDouble();
            case STRING ->
                    textAsBinary
                            ? input.readPayload(input.unpackRawStringHeader())
                            : input.unpackString();
            case BINARY -> input.readPayload(input.unpackBinaryHeader());
            case ARRAY ->
                    readArray(input, input.unpackArrayHeader(), bodyBytes, depth + 1, textAsBinary);
            case MAP -> readMap(input, input.unpackMapHeader(), bodyBytes, depth + 1, textAsBinary);
            case EXTENSION ->
                    throw new IOException(
                            "MessagePack extension values are not supported in command responses");
        };
    }

    private static List<Object> readArray(
            MessageUnpacker input, int count, int bodyBytes, int depth, boolean textAsBinary)
            throws IOException {
        requireCollectionSize(count, bodyBytes, "array");
        List<Object> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(readValue(input, bodyBytes, depth, textAsBinary));
        }
        return result;
    }

    private static Map<Object, Object> readMap(
            MessageUnpacker input, int count, int bodyBytes, int depth, boolean textAsBinary)
            throws IOException {
        requireCollectionSize(count, bodyBytes / 2, "map");
        Map<Object, Object> result = new LinkedHashMap<>(mapCapacity(count));
        for (int index = 0; index < count; index++) {
            Object key = readValue(input, bodyBytes, depth, textAsBinary);
            Object value = readValue(input, bodyBytes, depth, textAsBinary);
            result.put(key, value);
        }
        return result;
    }

    private static void requireNext(MessageUnpacker input) throws IOException {
        if (!input.hasNext()) {
            throw new IOException("unexpected end of MessagePack response");
        }
    }

    private static Object narrow(BigInteger value) {
        return value.bitLength() < Long.SIZE ? value.longValue() : value;
    }

    private static void requireDepth(int depth) {
        if (depth > MAX_DEPTH) {
            throw unsupported("MessagePack command nesting exceeds " + MAX_DEPTH);
        }
    }

    private static void requireCollectionSize(int count, int maximum, String type)
            throws IOException {
        if (count < 0 || count > maximum) {
            throw new IOException("MessagePack " + type + " declares an impossible size");
        }
    }

    private static void requireFinite(double value) {
        if (!Double.isFinite(value)) {
            throw unsupported("HTTP command floats must be finite");
        }
    }

    private static int mapCapacity(int entries) {
        if (entries < 3) {
            return 4;
        }
        long capacity = entries * 4L / 3L + 1L;
        return (int) Math.min(1 << 30, capacity);
    }

    private static IllegalArgumentException unsupported(String message) {
        return new IllegalArgumentException(message);
    }

    @FunctionalInterface
    interface MessageWriter {
        void write(MessagePacker output) throws IOException;
    }
}
