package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class NativeValueCodecTest {
    @Test
    void encodesProtocolV1TypedValuesExactly() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("nil", null);
        value.put("flags", List.of(true, false));
        value.put("number", -2L);
        value.put("ratio", 1.5d);
        value.put("bytes", new byte[] {0x41, 0x42});

        byte[] encoded = NativeValueCodec.encode(value, 1_024);

        assertArrayEquals(
                new byte[] {
                    6,
                    0,
                    0,
                    0,
                    5,
                    0,
                    0,
                    0,
                    3,
                    'n',
                    'i',
                    'l',
                    0,
                    0,
                    0,
                    0,
                    5,
                    'f',
                    'l',
                    'a',
                    'g',
                    's',
                    5,
                    0,
                    0,
                    0,
                    2,
                    1,
                    2,
                    0,
                    0,
                    0,
                    6,
                    'n',
                    'u',
                    'm',
                    'b',
                    'e',
                    'r',
                    3,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -2,
                    0,
                    0,
                    0,
                    5,
                    'r',
                    'a',
                    't',
                    'i',
                    'o',
                    7,
                    0x3f,
                    (byte) 0xf8,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    5,
                    'b',
                    'y',
                    't',
                    'e',
                    's',
                    4,
                    0,
                    0,
                    0,
                    2,
                    0x41,
                    0x42
                },
                encoded);

        @SuppressWarnings("unchecked")
        Map<String, Object> decoded = (Map<String, Object>) NativeValueCodec.decode(encoded);
        assertEquals(null, decoded.get("nil"));
        assertEquals(List.of(true, false), decoded.get("flags"));
        assertEquals(-2L, decoded.get("number"));
        assertEquals(1.5d, decoded.get("ratio"));
        assertArrayEquals(new byte[] {0x41, 0x42}, (byte[]) decoded.get("bytes"));
    }

    @Test
    void rejectsOversizedAndMalformedValuesBeforeUnboundedAllocation() {
        assertThrows(
                NativeProtocolException.class,
                () -> NativeValueCodec.encode(Map.of("value", "too large"), 8));
        assertThrows(
                NativeProtocolException.class,
                () -> NativeValueCodec.decode(new byte[] {4, 0, 0, 1, 0}));
        assertThrows(
                NativeProtocolException.class,
                () ->
                        NativeValueCodec.decode(
                                new byte[] {
                                    6, 0, 0, 0, 2, 0, 0, 0, 1, 'x', 0, 0, 0, 0, 1, 'x', 0
                                }));
    }
}
