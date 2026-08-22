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
