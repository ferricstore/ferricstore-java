package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

final class HashSlot {
    private static final int SLOT_MASK = 0x3fff;

    private HashSlot() {}

    static void requireSame(String command, List<String> keys) {
        if (keys.size() < 2) {
            return;
        }
        int expected = of(keys.getFirst());
        for (int index = 1; index < keys.size(); index++) {
            if (of(keys.get(index)) != expected) {
                throw new IllegalArgumentException(
                        command + " requires keys in one FerricStore hash slot");
            }
        }
    }

    static int of(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("FerricStore keys must not be empty");
        }
        String hashInput = hashTag(key);
        CRC32 crc = new CRC32();
        crc.update(hashInput.getBytes(StandardCharsets.UTF_8));
        return (int) crc.getValue() & SLOT_MASK;
    }

    private static String hashTag(String key) {
        int start = key.indexOf('{');
        if (start < 0) {
            return key;
        }
        int end = key.indexOf('}', start + 1);
        return end > start + 1 ? key.substring(start + 1, end) : key;
    }
}
