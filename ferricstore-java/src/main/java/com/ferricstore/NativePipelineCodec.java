package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

/** Compact homogeneous GET/SET pipeline request codec. */
final class NativePipelineCodec {
    private static final int MARKER = 0x94;
    private static final int SET_MODE = 1;
    private static final int GET_MODE = 2;
    private static final int HEADER_BYTES = 6;
    private static final int MAX_BYTES = 64 * 1024 * 1024;
    private static final String MALFORMED_UTF8 =
            "compact native pipeline strings must contain valid UTF-8";
    private static final String TOO_LARGE =
            "compact native pipeline exceeds the maximum request size";

    private NativePipelineCodec() {}

    static Encoded encode(List<List<Object>> commands) {
        Encoded encoded = tryEncodeDetailed(commands);
        if (encoded == null) {
            throw new IllegalArgumentException("commands do not form a compact native pipeline");
        }
        return encoded;
    }

    static byte[] tryEncode(List<List<Object>> commands) {
        Encoded encoded = tryEncodeDetailed(commands);
        return encoded == null ? null : encoded.payload();
    }

    @SuppressWarnings("PMD.ForLoopCanBeForeach") // Indexed traversal avoids hot-path iterators.
    static Encoded tryEncodeDetailed(List<List<Object>> commands) {
        if (commands == null || commands.isEmpty()) {
            return null;
        }
        int mode = mode(commands.get(0));
        if (mode == 0) {
            return null;
        }
        int commandSize = mode == GET_MODE ? 2 : 3;
        List<List<Object>> indexed =
                commands instanceof RandomAccess ? commands : new ArrayList<>(commands);

        long encodedBytes = HEADER_BYTES;
        for (int index = 0; index < indexed.size(); index++) {
            List<Object> command = indexed.get(index);
            if (command == null
                    || command.size() != commandSize
                    || !matchesMode(command.get(0), mode)) {
                return null;
            }
            int keyBytes = binaryLength(command.get(1));
            if (keyBytes < 0) {
                return null;
            }
            encodedBytes = addBinary(encodedBytes, keyBytes);
            if (commandSize == 3) {
                int valueBytes = binaryLength(command.get(2));
                if (valueBytes < 0) {
                    return null;
                }
                encodedBytes = addBinary(encodedBytes, valueBytes);
            }
        }

        byte[] payload = new byte[(int) encodedBytes];
        payload[0] = (byte) MARKER;
        payload[1] = (byte) mode;
        writeInt(payload, 2, commands.size());
        int offset = HEADER_BYTES;
        for (int index = 0; index < indexed.size(); index++) {
            List<Object> command = indexed.get(index);
            offset = writeBinary(payload, offset, command.get(1));
            if (commandSize == 3) {
                offset = writeBinary(payload, offset, command.get(2));
            }
        }
        return new Encoded(mode, commands.size(), payload);
    }

    private static int mode(List<Object> command) {
        if (command == null || command.isEmpty()) {
            return 0;
        }
        Object value = command.get(0);
        if (value instanceof String text) {
            if ("GET".equalsIgnoreCase(text)) {
                return GET_MODE;
            }
            if ("SET".equalsIgnoreCase(text)) {
                return SET_MODE;
            }
            return 0;
        }
        if (!(value instanceof byte[] bytes) || bytes.length != 3) {
            return 0;
        }
        if (asciiEqualsIgnoreCase(bytes, 'g', 'e', 't')) {
            return GET_MODE;
        }
        return asciiEqualsIgnoreCase(bytes, 's', 'e', 't') ? SET_MODE : 0;
    }

    private static boolean matchesMode(Object value, int mode) {
        if (value instanceof String text) {
            String expected = mode == GET_MODE ? "GET" : "SET";
            return expected.equals(text) || expected.equalsIgnoreCase(text);
        }
        if (!(value instanceof byte[] bytes) || bytes.length != 3) {
            return false;
        }
        return mode == GET_MODE
                ? asciiEqualsIgnoreCase(bytes, 'g', 'e', 't')
                : asciiEqualsIgnoreCase(bytes, 's', 'e', 't');
    }

    private static int binaryLength(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        if (value instanceof String text) {
            return NativeUtf8.length(text, MAX_BYTES, MALFORMED_UTF8, TOO_LARGE);
        }
        return -1;
    }

    private static long addBinary(long current, int valueBytes) {
        long result = current + Integer.BYTES + valueBytes;
        if (result > MAX_BYTES) {
            throw tooLarge();
        }
        return result;
    }

    private static int writeBinary(byte[] target, int offset, Object value) {
        int length = binaryLength(value);
        writeInt(target, offset, length);
        int valueOffset = offset + Integer.BYTES;
        if (value instanceof byte[] bytes) {
            System.arraycopy(bytes, 0, target, valueOffset, bytes.length);
        } else {
            NativeUtf8.write(target, valueOffset, (String) value);
        }
        return valueOffset + length;
    }

    private static boolean asciiEqualsIgnoreCase(byte[] value, int first, int second, int third) {
        return (value[0] | 0x20) == first
                && (value[1] | 0x20) == second
                && (value[2] | 0x20) == third;
    }

    private static void writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static NativeProtocolException tooLarge() {
        return new NativeProtocolException(TOO_LARGE);
    }

    record Encoded(int mode, int count, byte[] payload) {}
}
