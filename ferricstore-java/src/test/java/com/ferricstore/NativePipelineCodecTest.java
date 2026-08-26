package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NativePipelineCodecTest {
    @Test
    void encodesHomogeneousGetPipelineUsingTheCompactWireMode() throws Exception {
        byte[] encoded =
                NativePipelineCodec.tryEncode(
                        List.of(List.of("GET", "a"), List.of("GET", new byte[] {0, 1})));

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(expected)) {
            output.writeByte(0x94);
            output.writeByte(2);
            output.writeInt(2);
            writeBinary(output, "a".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, new byte[] {0, 1});
        }
        assertArrayEquals(expected.toByteArray(), encoded);
    }

    @Test
    void encodesHomogeneousSetPipelineAndRejectsUnsupportedShapes() throws Exception {
        byte[] encoded =
                NativePipelineCodec.tryEncode(
                        List.of(List.of("SET", "a", new byte[] {1}), List.of("SET", "b", "v")));

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(expected)) {
            output.writeByte(0x94);
            output.writeByte(1);
            output.writeInt(2);
            writeBinary(output, "a".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, new byte[] {1});
            writeBinary(output, "b".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, "v".getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(expected.toByteArray(), encoded);
        assertNull(
                NativePipelineCodec.tryEncode(
                        List.of(List.of("GET", "a"), List.of("SET", "a", "v"))));
        assertNull(NativePipelineCodec.tryEncode(List.of(List.of("SET", "a", "v", "NX"))));
    }

    @Test
    void returnsThePipelineItemCount() {
        NativePipelineCodec.Encoded encoded =
                NativePipelineCodec.encode(List.of(List.of("GET", "a"), List.of("GET", "b")));

        assertEquals(2, encoded.count());
        assertEquals(2, encoded.mode());
    }

    @Test
    void preservesUtf8AndCaseInsensitiveCommandWireSemantics() throws Exception {
        byte[] encoded =
                NativePipelineCodec.tryEncode(
                        List.of(
                                List.of("get", "שלום-😀"),
                                List.of("GeT".getBytes(StandardCharsets.UTF_8), "café")));

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(expected)) {
            output.writeByte(0x94);
            output.writeByte(2);
            output.writeInt(2);
            writeBinary(output, "שלום-😀".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, "café".getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(expected.toByteArray(), encoded);
    }

    @Test
    void rejectsMalformedUtf16WithoutWritingAPartialPayload() {
        String malformed = "key-\ud800";

        assertThrows(
                NativeProtocolException.class,
                () -> NativePipelineCodec.tryEncode(List.of(List.of("GET", malformed))));
    }

    private static void writeBinary(DataOutputStream output, byte[] value) throws Exception {
        output.writeInt(value.length);
        output.write(value);
    }
}
