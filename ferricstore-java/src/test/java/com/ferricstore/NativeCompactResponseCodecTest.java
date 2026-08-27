package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NativeCompactResponseCodecTest {
    @Test
    void decodesCompactOkListsWithoutTrustingTheWireCount() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x81);
            output.writeInt(3);
        }

        NativeResponseCodec.Response response =
                NativeCompactResponseCodec.decode("ok_list_v1", bytes.toByteArray());

        assertEquals(
                List.of("OK", "OK", "OK"),
                ((List<?>) response.value())
                        .stream()
                                .map(
                                        value ->
                                                new String(
                                                        (byte[]) value,
                                                        java.nio.charset.StandardCharsets.UTF_8))
                                .toList());
        bytes.reset();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x81);
            output.writeInt(Integer.MAX_VALUE);
        }
        assertThrows(
                NativeProtocolException.class,
                () -> NativeCompactResponseCodec.decode("ok_list_v1", bytes.toByteArray()));
    }

    @Test
    void decodesCompactKvMgetWithBinaryValuesAndMissingEntries() throws Exception {
        byte[] body = compactMgetBody(List.of(new byte[] {0, 1, 2}, Missing.VALUE, new byte[0]));

        NativeResponseCodec.Response response =
                NativeCompactResponseCodec.decode("kv_mget_v1", body);

        assertEquals(NativeProtocol.STATUS_OK, response.status());
        List<?> values = (List<?>) response.value();
        assertArrayEquals(new byte[] {0, 1, 2}, (byte[]) values.get(0));
        assertNull(values.get(1));
        assertArrayEquals(new byte[0], (byte[]) values.get(2));
    }

    @Test
    void rejectsMalformedCompactKvMgetWithoutAllocatingFromAnUntrustedCount() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x83);
            output.writeInt(Integer.MAX_VALUE);
        }

        assertThrows(
                NativeProtocolException.class,
                () -> NativeCompactResponseCodec.decode("kv_mget_v1", bytes.toByteArray()));
    }

    @Test
    void rejectsAResponseMarkerThatDoesNotMatchTheHelloCodec() throws Exception {
        byte[] body = compactMgetBody(List.of(new byte[] {1}));
        body[2] = (byte) 0x82;

        assertThrows(
                NativeProtocolException.class,
                () -> NativeCompactResponseCodec.decode("kv_mget_v1", body));
    }

    @Test
    void rejectsFixedWidthResponsesWithAnUntrustedZeroWidthCount() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x89);
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(0);
        }

        assertThrows(
                NativeProtocolException.class,
                () -> NativeCompactResponseCodec.decode("kv_mget_v1", bytes.toByteArray()));
    }

    @Test
    void decodesCompactPipelineBinaryNilAndErrorItems() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x95);
            output.writeInt(3);
            output.writeByte(0);
            output.writeByte(1);
            output.writeInt(2);
            output.write(new byte[] {1, 2});
            output.writeByte(0);
            output.writeByte(0);
            output.writeByte(2);
            output.writeInt(4);
            output.write("nope".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        NativeResponseCodec.Response response =
                NativeCompactResponseCodec.decode("pipeline_v1", bytes.toByteArray());

        NativeCompactResponseCodec.PipelineResults decoded =
                (NativeCompactResponseCodec.PipelineResults) response.value();
        assertArrayEquals(new byte[] {1, 2}, (byte[]) decoded.values().get(0));
        assertNull(decoded.values().get(1));
        assertNull(decoded.values().get(2));
        assertEquals("error", decoded.firstFailure().status());
        assertEquals(2, decoded.firstFailure().index());
        assertArrayEquals(
                "nope".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                (byte[]) decoded.firstFailure().value());
    }

    @Test
    void rejectsCompactPipelineUnsignedCountsBeyondTheCollectionLimit() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x95);
            output.writeInt(-1);
        }

        assertThrows(
                NativeProtocolException.class,
                () -> NativeCompactResponseCodec.decode("pipeline_v1", bytes.toByteArray()));
    }

    private static byte[] compactMgetBody(List<?> values) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x83);
            output.writeInt(values.size());
            for (Object value : values) {
                if (value == Missing.VALUE) {
                    output.writeByte(0);
                } else {
                    byte[] binary = (byte[]) value;
                    output.writeByte(1);
                    output.writeInt(binary.length);
                    output.write(binary);
                }
            }
        }
        return bytes.toByteArray();
    }

    private enum Missing {
        VALUE
    }
}
