package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

final class NativeFrameTest {
    @Test
    void keepsTheNativeWireAtProtocolV1() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        NativeFrame.writeRequest(
                output, 7, NativeProtocol.OP_COMMAND_EXEC, 9, 0, new byte[] {1, 2});

        assertArrayEquals(
                new byte[] {
                    'F', 'S', 'N', 'P', 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 2,
                    1, 2
                },
                output.toByteArray());
    }

    @Test
    void rejectsAFrameFromItsLengthHeaderBeforeReadingOrAllocatingItsBody() {
        byte[] header = {
            'F', 'S', 'N', 'P', (byte) 0x81, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0
        };

        NativeProtocolException error =
                assertThrows(
                        NativeProtocolException.class,
                        () -> NativeFrame.readResponse(new ByteArrayInputStream(header), 64));
        assertEquals("native response frame exceeds max_response_bytes", error.getMessage());
    }

    @Test
    void reassemblesInterleavedChunksByFullIdentityAndBoundsTheAggregate() {
        NativeResponseAssembler assembler = new NativeResponseAssembler(8, 4);
        NativeFrame.Identity first = new NativeFrame.Identity(1, 0x0104, 11);
        NativeFrame.Identity second = new NativeFrame.Identity(2, 0x020C, 12);

        assertNull(assembler.add(first, NativeProtocol.FLAG_MORE_CHUNKS, new byte[] {1, 2}));
        assertNull(assembler.add(second, NativeProtocol.FLAG_MORE_CHUNKS, new byte[] {3}));
        NativeResponseAssembler.Assembled firstDone = assembler.add(first, 0, new byte[] {4, 5});
        NativeResponseAssembler.Assembled secondDone = assembler.add(second, 0, new byte[] {6});

        assertEquals(first, firstDone.identity());
        assertArrayEquals(new byte[] {1, 2, 4, 5}, firstDone.body());
        assertEquals(0, firstDone.flags() & NativeProtocol.FLAG_MORE_CHUNKS);
        assertEquals(second, secondDone.identity());
        assertArrayEquals(new byte[] {3, 6}, secondDone.body());

        NativeFrame.Identity oversized = new NativeFrame.Identity(3, 0x0104, 13);
        assertNull(
                assembler.add(
                        oversized, NativeProtocol.FLAG_MORE_CHUNKS, new byte[] {1, 2, 3, 4, 5}));
        assertThrows(
                NativeProtocolException.class,
                () -> assembler.add(oversized, 0, new byte[] {6, 7, 8, 9}));
        assertEquals(0, assembler.pendingCount());
    }
}
