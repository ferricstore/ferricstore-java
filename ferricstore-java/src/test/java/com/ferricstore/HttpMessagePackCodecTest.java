package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class HttpMessagePackCodecTest {
    @Test
    void roundTripsNestedBinaryValuesAndNonStringMapKeys() throws IOException {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(
                new byte[] {0, (byte) 0xff},
                List.of(ByteBuffer.wrap(new byte[] {(byte) 0x80}), 42L));
        Map<String, Object> envelope =
                Map.of(
                        "encoding",
                        HttpMessagePackCodec.ENCODING,
                        "results",
                        List.of(Map.of("status", "ok", "value", nested)));

        byte[] encoded =
                HttpMessagePackCodec.encode(
                        output -> HttpMessagePackCodec.writeValue(output, envelope));
        Map<String, Object> decoded = HttpMessagePackCodec.decode(encoded);

        assertEquals(HttpMessagePackCodec.ENCODING, decoded.get("encoding"));
        List<?> results = assertInstanceOf(List.class, decoded.get("results"));
        Map<?, ?> result = assertInstanceOf(Map.class, results.get(0));
        Map<?, ?> value = assertInstanceOf(Map.class, result.get("value"));
        Map.Entry<?, ?> entry = value.entrySet().iterator().next();
        assertArrayEquals(
                new byte[] {0, (byte) 0xff}, assertInstanceOf(byte[].class, entry.getKey()));
        List<?> items = assertInstanceOf(List.class, entry.getValue());
        assertArrayEquals(new byte[] {(byte) 0x80}, assertInstanceOf(byte[].class, items.get(0)));
        assertEquals(42L, items.get(1));
    }

    @Test
    void rejectsMalformedTrailingAndNonMapResponses() throws IOException {
        assertThrows(
                IOException.class, () -> HttpMessagePackCodec.decode(new byte[] {(byte) 0x81}));

        byte[] list =
                HttpMessagePackCodec.encode(
                        output -> HttpMessagePackCodec.writeValue(output, List.of("value")));
        assertThrows(IOException.class, () -> HttpMessagePackCodec.decode(list));

        byte[] trailing =
                HttpMessagePackCodec.encode(
                        output -> {
                            HttpMessagePackCodec.writeValue(output, Map.of("value", 1));
                            HttpMessagePackCodec.writeValue(output, Map.of("value", 2));
                        });
        assertThrows(IOException.class, () -> HttpMessagePackCodec.decode(trailing));
    }

    @Test
    void responseValuesRestoreNativeBinarySemanticsWithoutChangingMetadata() throws IOException {
        byte[] encoded =
                HttpMessagePackCodec.encode(
                        output ->
                                HttpMessagePackCodec.writeValue(
                                        output,
                                        Map.of(
                                                "encoding",
                                                HttpMessagePackCodec.ENCODING,
                                                "results",
                                                List.of(
                                                        Map.of(
                                                                "status",
                                                                "ok",
                                                                "value",
                                                                Map.of("field", "value"))))));

        Map<String, Object> response = HttpMessagePackCodec.decodeResponse(encoded);
        assertEquals(HttpMessagePackCodec.ENCODING, response.get("encoding"));
        List<?> results = assertInstanceOf(List.class, response.get("results"));
        Map<?, ?> result = assertInstanceOf(Map.class, results.get(0));
        assertEquals("ok", result.get("status"));
        Map<?, ?> value = assertInstanceOf(Map.class, result.get("value"));
        Map.Entry<?, ?> entry = value.entrySet().iterator().next();
        assertArrayEquals(
                "field".getBytes(StandardCharsets.UTF_8),
                assertInstanceOf(byte[].class, entry.getKey()));
        assertArrayEquals(
                "value".getBytes(StandardCharsets.UTF_8),
                assertInstanceOf(byte[].class, entry.getValue()));
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // The loop builds a nested fixture.
    void rejectsNonFiniteNumbersAndExcessiveNesting() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        HttpMessagePackCodec.encode(
                                output -> HttpMessagePackCodec.writeValue(output, Double.NaN)));

        Object nested = "leaf";
        for (int index = 0; index < 102; index++) {
            nested = new ArrayList<>(List.of(nested));
        }
        Object excessive = nested;
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        HttpMessagePackCodec.encode(
                                output -> HttpMessagePackCodec.writeValue(output, excessive)));
    }
}
