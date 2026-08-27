package com.ferricstore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Decodes compact response bodies declared by the server's HELLO capabilities. */
final class NativeCompactResponseCodec {
    private static final int COMPACT_KV_MGET = 0x83;
    private static final int COMPACT_KV_MGET_FIXED = 0x89;
    private static final int COMPACT_OK_LIST = 0x81;
    private static final int COMPACT_PIPELINE = 0x95;
    private static final int MAX_COLLECTION_ITEMS = 1_000_000;

    private NativeCompactResponseCodec() {}

    static NativeResponseCodec.Response decode(String codec, byte[] body) {
        if (body == null || body.length < 3) {
            throw malformed(codec);
        }
        int status = (Byte.toUnsignedInt(body[0]) << 8) | Byte.toUnsignedInt(body[1]);
        if (status != NativeProtocol.STATUS_OK) {
            return NativeResponseCodec.decode(body);
        }
        if ("pipeline_v1".equals(codec)) {
            return new NativeResponseCodec.Response(status, decodePipeline(body, codec));
        }
        if ("ok_list_v1".equals(codec)) {
            return new NativeResponseCodec.Response(status, decodeOkList(body, codec));
        }
        if (!"kv_mget_v1".equals(codec)) {
            throw new NativeProtocolException(
                    "unsupported negotiated native response codec: " + codec);
        }

        ByteBuffer input = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        input.position(2);
        int marker = Byte.toUnsignedInt(input.get());
        Object value =
                switch (marker) {
                    case COMPACT_KV_MGET -> decodeKvMget(input, codec);
                    case COMPACT_KV_MGET_FIXED -> decodeFixedKvMget(input, codec);
                    default -> throw malformed(codec);
                };
        return new NativeResponseCodec.Response(status, value);
    }

    private static List<Object> decodeOkList(byte[] body, String codec) {
        ByteBuffer input = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        input.position(2);
        require(input, 1, codec);
        if (Byte.toUnsignedInt(input.get()) != COMPACT_OK_LIST) {
            throw malformed(codec);
        }
        int count = readCollectionCount(input, codec);
        requireFullyConsumed(input, codec);
        List<Object> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(new byte[] {'O', 'K'});
        }
        return Collections.unmodifiableList(values);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static PipelineResults decodePipeline(byte[] body, String codec) {
        ByteBuffer input = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        input.position(2);
        require(input, 1, codec);
        if (Byte.toUnsignedInt(input.get()) != COMPACT_PIPELINE) {
            throw malformed(codec);
        }
        int count = readCollectionCount(input, codec);
        if (count > input.remaining()) {
            throw malformed(codec);
        }
        List<Object> results = new ArrayList<>(count);
        PipelineFailure firstFailure = null;
        for (int index = 0; index < count; index++) {
            require(input, 1, codec);
            int status = Byte.toUnsignedInt(input.get());
            if (status == 0) {
                require(input, 1, codec);
                int present = Byte.toUnsignedInt(input.get());
                if (present == 0) {
                    results.add(null);
                } else if (present == 1) {
                    results.add(readBinary(input, codec));
                } else {
                    throw malformed(codec);
                }
            } else if (status == 1 || status == 2) {
                byte[] reason = readBinary(input, codec);
                if (firstFailure == null) {
                    firstFailure =
                            new PipelineFailure(index, status == 1 ? "busy" : "error", reason);
                }
                results.add(null);
            } else {
                throw malformed(codec);
            }
        }
        requireFullyConsumed(input, codec);
        return new PipelineResults(Collections.unmodifiableList(results), firstFailure);
    }

    private static List<Object> decodeKvMget(ByteBuffer input, String codec) {
        long count = readUnsignedInt(input, codec);
        if (count > MAX_COLLECTION_ITEMS || count > input.remaining()) {
            throw malformed(codec);
        }
        int itemCount = (int) count;
        List<Object> values = new ArrayList<>(itemCount);
        for (int index = 0; index < itemCount; index++) {
            require(input, 1, codec);
            int present = Byte.toUnsignedInt(input.get());
            if (present == 0) {
                values.add(null);
            } else if (present == 1) {
                values.add(readBinary(input, codec));
            } else {
                throw malformed(codec);
            }
        }
        requireFullyConsumed(input, codec);
        return Collections.unmodifiableList(values);
    }

    private static List<Object> decodeFixedKvMget(ByteBuffer input, String codec) {
        long count = readUnsignedInt(input, codec);
        long size = readUnsignedInt(input, codec);
        long payloadBytes;
        try {
            payloadBytes = Math.multiplyExact(count, size);
        } catch (ArithmeticException error) {
            throw malformed(codec, error);
        }
        if (count > MAX_COLLECTION_ITEMS
                || size > Integer.MAX_VALUE
                || payloadBytes != input.remaining()) {
            throw malformed(codec);
        }
        int itemCount = (int) count;
        int itemSize = (int) size;
        List<Object> values = new ArrayList<>(itemCount);
        for (int index = 0; index < itemCount; index++) {
            values.add(readFixedBinary(input, itemSize));
        }
        return Collections.unmodifiableList(values);
    }

    private static byte[] readBinary(ByteBuffer input, String codec) {
        long length = readUnsignedInt(input, codec);
        if (length > input.remaining()) {
            throw malformed(codec);
        }
        byte[] value = new byte[(int) length];
        input.get(value);
        return value;
    }

    private static byte[] readFixedBinary(ByteBuffer input, int size) {
        byte[] value = new byte[size];
        input.get(value);
        return value;
    }

    private static long readUnsignedInt(ByteBuffer input, String codec) {
        require(input, Integer.BYTES, codec);
        return Integer.toUnsignedLong(input.getInt());
    }

    private static int readCollectionCount(ByteBuffer input, String codec) {
        long count = readUnsignedInt(input, codec);
        if (count > MAX_COLLECTION_ITEMS) {
            throw malformed(codec);
        }
        return (int) count;
    }

    private static void require(ByteBuffer input, int bytes, String codec) {
        if (bytes < 0 || input.remaining() < bytes) {
            throw malformed(codec);
        }
    }

    private static void requireFullyConsumed(ByteBuffer input, String codec) {
        if (input.hasRemaining()) {
            throw malformed(codec);
        }
    }

    private static NativeProtocolException malformed(String codec) {
        return new NativeProtocolException("malformed native " + codec + " response payload");
    }

    private static NativeProtocolException malformed(String codec, Throwable cause) {
        return new NativeProtocolException(
                "malformed native " + codec + " response payload", cause);
    }

    record PipelineResults(List<Object> values, PipelineFailure firstFailure) {}

    record PipelineFailure(int index, String status, Object value) {}
}
