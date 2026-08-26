package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NativeFlowManyCodecTest {
    @Test
    void encodesCompactTransitionManyAndDefaultsRunAtToTheExplicitNow() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.TRANSITION_MANY",
                        List.of(
                                "MIXED",
                                "running",
                                "done",
                                "NOW",
                                100L,
                                "INDEPENDENT",
                                true,
                                "RETURN",
                                "OK_ON_SUCCESS",
                                "ITEMS",
                                "a",
                                "p1",
                                1L,
                                "lease-a"));

        NativeFlowManyCodec.Encoded encoded = NativeFlowManyCodec.tryEncode(prepared);

        ByteBuffer input = ByteBuffer.wrap(encoded.payload()).order(ByteOrder.BIG_ENDIAN);
        assertEquals(0x9C, Byte.toUnsignedInt(input.get()));
        assertArrayEquals(bytes("running"), binary(input));
        assertArrayEquals(bytes("done"), binary(input));
        assertNull(optionalBinary(input));
        assertEquals(100L, input.getLong());
        assertEquals(100L, input.getLong());
        assertEquals(2, Byte.toUnsignedInt(input.get()));
        assertEquals(1, input.getInt());
        assertArrayEquals(bytes("a"), binary(input));
        assertArrayEquals(bytes("p1"), optionalBinary(input));
        assertEquals(1L, input.getLong());
        assertArrayEquals(bytes("lease-a"), optionalBinary(input));
        assertEquals(0, input.remaining());
    }

    @Test
    void encodesCompactCompleteManyOkResponseShape() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.COMPLETE_MANY",
                        List.of(
                                "tenant-a",
                                "NOW",
                                100L,
                                "INDEPENDENT",
                                true,
                                "RETURN",
                                "OK_ON_SUCCESS",
                                "ITEMS",
                                "a",
                                "lease-a",
                                1L));

        NativeFlowManyCodec.Encoded encoded = NativeFlowManyCodec.tryEncode(prepared);

        ByteBuffer input = ByteBuffer.wrap(encoded.payload()).order(ByteOrder.BIG_ENDIAN);
        assertEquals(0x93, Byte.toUnsignedInt(input.get()));
        assertArrayEquals(bytes("tenant-a"), optionalBinary(input));
        assertEquals(100L, input.getLong());
        assertEquals(2, Byte.toUnsignedInt(input.get()));
        assertEquals(1, input.getInt());
        assertArrayEquals(bytes("a"), binary(input));
        assertNull(optionalBinary(input));
        assertArrayEquals(bytes("lease-a"), binary(input));
        assertEquals(1L, input.getLong());
        assertEquals(0, input.remaining());
    }

    @Test
    void fallsBackWhenOptionsWouldBeLostByTheCompactContract() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.COMPLETE_MANY",
                        List.of(
                                "MIXED",
                                "RESULT",
                                bytes("value"),
                                "NOW",
                                100L,
                                "ITEMS",
                                "a",
                                "p1",
                                "lease-a",
                                1L));

        assertNull(NativeFlowManyCodec.tryEncode(prepared));
    }

    @Test
    void rejectsMalformedUtf16WithoutReplacingWireBytes() {
        FlowManyCommandEncoder.Prepared prepared =
                FlowManyCommandEncoder.tryPrepare(
                        "FLOW.TRANSITION_MANY",
                        List.of(
                                "MIXED",
                                "running",
                                "done",
                                "NOW",
                                100L,
                                "ITEMS",
                                "bad-\ud800",
                                "p1",
                                1L,
                                "lease-a"));

        assertThrows(NativeProtocolException.class, () -> NativeFlowManyCodec.tryEncode(prepared));
    }

    private static byte[] binary(ByteBuffer input) {
        int length = input.getInt();
        byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static byte[] optionalBinary(ByteBuffer input) {
        int length = input.getInt();
        if (length == -1) {
            return null;
        }
        byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
