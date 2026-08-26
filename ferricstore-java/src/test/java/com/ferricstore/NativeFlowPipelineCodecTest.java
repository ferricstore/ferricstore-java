package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NativeFlowPipelineCodecTest {
    @Test
    void convertsHomogeneousCreatesIntoOneCompactCreateManyRequest() throws Exception {
        NativeFlowPipelineCodec.Encoded encoded =
                NativeFlowPipelineCodec.tryEncodeCreateMany(
                        List.of(
                                create("id-a", new byte[] {0, 1}),
                                create("id-b", new byte[] {(byte) 0xff})));

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(expected)) {
            output.writeByte(0x90);
            writeBinary(output, "type".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, "queued".getBytes(StandardCharsets.UTF_8));
            output.writeLong(123L);
            output.writeLong(123L);
            output.writeByte(2);
            output.writeByte(1);
            output.writeInt(2);
            writeBinary(output, "id-a".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, new byte[] {0, 1});
            writeBinary(output, "id-b".getBytes(StandardCharsets.UTF_8));
            writeBinary(output, new byte[] {(byte) 0xff});
        }
        assertEquals(2, encoded.count());
        assertArrayEquals(expected.toByteArray(), encoded.payload());
    }

    @Test
    void declinesShapesThatCannotPreserveCreateSemantics() {
        assertNull(
                NativeFlowPipelineCodec.tryEncodeCreateMany(
                        List.of(createWithoutPayload("id-a"), createWithoutPayload("id-b"))));
        assertNull(
                NativeFlowPipelineCodec.tryEncodeCreateMany(
                        List.of(create("id-a", new byte[0]), createAt("id-b", 124L))));
        assertNull(
                NativeFlowPipelineCodec.tryEncodeCreateMany(
                        List.of(
                                List.of(
                                        "FLOW.CREATE",
                                        "id",
                                        "TYPE",
                                        "type",
                                        "STATE",
                                        "queued",
                                        "NOW",
                                        123L,
                                        "PARTITION",
                                        "p",
                                        "RUN_AT",
                                        123L))));
    }

    private static List<Object> create(String id, byte[] payload) {
        return List.of(
                "FLOW.CREATE",
                id,
                "TYPE",
                "type",
                "STATE",
                "queued",
                "NOW",
                123L,
                "PAYLOAD",
                payload,
                "RUN_AT",
                123L,
                "PRIORITY",
                0);
    }

    private static List<Object> createAt(String id, long now) {
        return List.of(
                "FLOW.CREATE",
                id,
                "TYPE",
                "type",
                "STATE",
                "queued",
                "NOW",
                now,
                "RUN_AT",
                now,
                "PRIORITY",
                0);
    }

    private static List<Object> createWithoutPayload(String id) {
        return List.of(
                "FLOW.CREATE",
                id,
                "TYPE",
                "type",
                "STATE",
                "queued",
                "NOW",
                123L,
                "RUN_AT",
                123L,
                "PRIORITY",
                0);
    }

    private static void writeBinary(DataOutputStream output, byte[] value) throws Exception {
        output.writeInt(value.length);
        output.write(value);
    }
}
