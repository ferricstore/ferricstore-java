package com.ferricstore;

import java.nio.charset.CharacterCodingException;

/** Strict allocation-free UTF-8 sizing and writing for compact native request codecs. */
final class NativeUtf8 {
    private NativeUtf8() {}

    static int length(String value, int maxBytes, String malformedMessage, String tooLargeMessage) {
        int bytes = 0;
        int index = 0;
        while (index < value.length()) {
            char character = value.charAt(index);
            if (character < 0x80) {
                bytes++;
            } else if (character < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(character)) {
                if (++index >= value.length() || !Character.isLowSurrogate(value.charAt(index))) {
                    throw malformed(malformedMessage);
                }
                bytes += 4;
            } else if (Character.isLowSurrogate(character)) {
                throw malformed(malformedMessage);
            } else {
                bytes += 3;
            }
            if (bytes > maxBytes) {
                throw new NativeProtocolException(tooLargeMessage);
            }
            index++;
        }
        return bytes;
    }

    static void write(byte[] target, int offset, String value) {
        int cursor = offset;
        int index = 0;
        while (index < value.length()) {
            char character = value.charAt(index);
            if (character < 0x80) {
                target[cursor++] = (byte) character;
            } else if (character < 0x800) {
                target[cursor++] = (byte) (0xc0 | character >> 6);
                target[cursor++] = (byte) (0x80 | character & 0x3f);
            } else if (Character.isHighSurrogate(character)) {
                int codePoint = Character.toCodePoint(character, value.charAt(++index));
                target[cursor++] = (byte) (0xf0 | codePoint >> 18);
                target[cursor++] = (byte) (0x80 | codePoint >> 12 & 0x3f);
                target[cursor++] = (byte) (0x80 | codePoint >> 6 & 0x3f);
                target[cursor++] = (byte) (0x80 | codePoint & 0x3f);
            } else {
                target[cursor++] = (byte) (0xe0 | character >> 12);
                target[cursor++] = (byte) (0x80 | character >> 6 & 0x3f);
                target[cursor++] = (byte) (0x80 | character & 0x3f);
            }
            index++;
        }
    }

    private static NativeProtocolException malformed(String message) {
        return new NativeProtocolException(message, new CharacterCodingException());
    }
}
