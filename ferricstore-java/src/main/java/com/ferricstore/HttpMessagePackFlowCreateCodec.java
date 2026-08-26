package com.ferricstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.msgpack.core.MessagePacker;

/** Allocation-conscious MessagePack writer for a homogeneous FLOW.CREATE pipeline. */
final class HttpMessagePackFlowCreateCodec {
    private static final int CREATE_MANY_PAYLOAD_FIELDS = 8;
    private static final byte[] COMMAND = bytes("command");
    private static final byte[] CREATE_MANY = bytes(FlowCommand.CREATE_MANY.wireName());
    private static final byte[] OPCODE = bytes("opcode");
    private static final byte[] PAYLOAD = bytes("payload");
    private static final byte[] ITEMS = bytes("items");
    private static final byte[] ID = bytes("id");
    private static final byte[] TYPE = bytes("type");
    private static final byte[] STATE = bytes("state");
    private static final byte[] NOW_MS = bytes("now_ms");
    private static final byte[] RUN_AT_MS = bytes("run_at_ms");
    private static final byte[] PRIORITY = bytes("priority");
    private static final byte[] INDEPENDENT = bytes("independent");
    private static final byte[] RETURN = bytes("return");
    private static final byte[] OK_ON_SUCCESS = bytes("ok_on_success");

    private HttpMessagePackFlowCreateCodec() {}

    static void writeCommand(MessagePacker output, FlowCreatePipeline.Batch batch)
            throws IOException {
        output.packMapHeader(3);
        writeText(output, COMMAND);
        writeText(output, CREATE_MANY);
        writeText(output, OPCODE);
        output.packInt(
                FlowCommand.CREATE_MANY
                        .nativeOpcode()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "FLOW.CREATE_MANY opcode is missing")));
        writeText(output, PAYLOAD);
        writePayload(output, batch);
    }

    private static void writePayload(MessagePacker output, FlowCreatePipeline.Batch batch)
            throws IOException {
        output.packMapHeader(CREATE_MANY_PAYLOAD_FIELDS);
        writeText(output, ITEMS);
        output.packArrayHeader(batch.count());
        for (FlowCreatePipeline.CreateItem item : batch.items()) {
            output.packMapHeader(item.payloadPresent() ? 2 : 1);
            writeText(output, ID);
            writeBytes(output, item.id());
            if (item.payloadPresent()) {
                writeText(output, PAYLOAD);
                writeBytes(output, item.payload());
            }
        }

        FlowCreatePipeline.Metadata metadata = batch.metadata();
        writeText(output, TYPE);
        writeBytes(output, metadata.type());
        writeText(output, STATE);
        writeBytes(output, metadata.state());
        writeText(output, NOW_MS);
        output.packLong(metadata.nowMs());
        writeText(output, RUN_AT_MS);
        output.packLong(metadata.runAtMs());
        writeText(output, PRIORITY);
        output.packLong(0L);
        writeText(output, INDEPENDENT);
        output.packBoolean(true);
        writeText(output, RETURN);
        writeText(output, OK_ON_SUCCESS);
    }

    private static void writeBytes(MessagePacker output, byte[] value) throws IOException {
        output.packBinaryHeader(value.length).writePayload(value);
    }

    private static void writeText(MessagePacker output, byte[] value) throws IOException {
        output.packRawStringHeader(value.length).writePayload(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
