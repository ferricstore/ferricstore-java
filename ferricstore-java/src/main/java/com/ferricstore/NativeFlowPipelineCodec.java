package com.ferricstore;

import java.util.List;

/** Compact homogeneous FLOW.CREATE pipeline encoder. */
final class NativeFlowPipelineCodec {
    private static final int CREATE_MANY_MARKER = 0x90;
    private static final int MAX_BYTES = 64 * 1024 * 1024;
    private static final int FIXED_BYTES = 1 + Long.BYTES * 2 + 2 + Integer.BYTES;

    private NativeFlowPipelineCodec() {}

    static Encoded tryEncodeCreateMany(List<List<Object>> commands) {
        return tryEncodeCreateMany(FlowCreatePipeline.tryParse(commands));
    }

    static Encoded tryEncodeCreateMany(FlowCreatePipeline.Batch batch) {
        if (batch == null || !batch.hasPayloadForEveryItem()) {
            return null;
        }
        List<FlowCreatePipeline.CreateItem> items = batch.items();
        FlowCreatePipeline.Metadata metadata = batch.metadata();
        long encodedBytes = FIXED_BYTES;
        encodedBytes = addBinary(encodedBytes, metadata.type().length);
        encodedBytes = addBinary(encodedBytes, metadata.state().length);
        for (FlowCreatePipeline.CreateItem item : items) {
            encodedBytes = addBinary(encodedBytes, item.id().length);
            encodedBytes = addBinary(encodedBytes, item.payload().length);
        }

        byte[] payload = new byte[(int) encodedBytes];
        int offset = 0;
        payload[offset++] = (byte) CREATE_MANY_MARKER;
        offset = writeBinary(payload, offset, metadata.type());
        offset = writeBinary(payload, offset, metadata.state());
        offset = writeLong(payload, offset, metadata.nowMs());
        offset = writeLong(payload, offset, metadata.runAtMs());
        payload[offset++] = 2; // independent=true
        payload[offset++] = 1; // return=OK_ON_SUCCESS
        offset = writeInt(payload, offset, items.size());
        for (FlowCreatePipeline.CreateItem item : items) {
            offset = writeBinary(payload, offset, item.id());
            offset = writeBinary(payload, offset, item.payload());
        }
        return new Encoded(items.size(), payload);
    }

    private static long addBinary(long current, int bytes) {
        long result = current + Integer.BYTES + bytes;
        if (result > MAX_BYTES) {
            throw new NativeProtocolException(
                    "compact FLOW.CREATE_MANY request exceeds the maximum request size");
        }
        return result;
    }

    private static int writeBinary(byte[] target, int offset, byte[] value) {
        int valueOffset = writeInt(target, offset, value.length);
        System.arraycopy(value, 0, target, valueOffset, value.length);
        return valueOffset + value.length;
    }

    private static int writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
        return offset + Integer.BYTES;
    }

    private static int writeLong(byte[] target, int offset, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            target[offset++] = (byte) (value >>> shift);
        }
        return offset;
    }

    record Encoded(int count, byte[] payload) {}
}
