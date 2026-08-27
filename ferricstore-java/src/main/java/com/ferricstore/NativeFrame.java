package com.ferricstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.IntSupplier;

record NativeFrame(Identity identity, int flags, byte[] body) {
    record Identity(long laneId, int opcode, long requestId) {
        Identity {
            if (laneId < 0 || laneId > 0xffff_ffffL) {
                throw new IllegalArgumentException("laneId must fit in an unsigned 32-bit value");
            }
            if (opcode < 0 || opcode > 0xffff) {
                throw new IllegalArgumentException("opcode must fit in an unsigned 16-bit value");
            }
            if (requestId < 0) {
                throw new IllegalArgumentException("requestId must be non-negative");
            }
        }
    }

    static void writeRequest(
            OutputStream output, long laneId, int opcode, long requestId, int flags, byte[] body)
            throws IOException {
        if (requestId == 0) {
            throw new IllegalArgumentException("native requests require a non-zero requestId");
        }
        Identity identity = new Identity(laneId, opcode, requestId);
        DataOutputStream data = new DataOutputStream(output);
        data.write(NativeProtocol.MAGIC);
        data.writeByte(NativeProtocol.REQUEST_VERSION);
        data.writeByte(flags);
        data.writeInt((int) identity.laneId());
        data.writeShort(identity.opcode());
        data.writeLong(identity.requestId());
        data.writeInt(body.length);
        data.write(body);
    }

    static NativeFrame readResponse(InputStream input, int maxFrameBytes) throws IOException {
        return readResponse(input, () -> maxFrameBytes);
    }

    static NativeFrame readResponse(InputStream input, IntSupplier maxFrameBytes)
            throws IOException {
        DataInputStream data = new DataInputStream(input);
        byte[] magic = new byte[NativeProtocol.MAGIC.length];
        data.readFully(magic);
        if (!Arrays.equals(NativeProtocol.MAGIC, magic)) {
            throw new NativeProtocolException("invalid native protocol magic");
        }
        int version = data.readUnsignedByte();
        if (version != NativeProtocol.RESPONSE_VERSION) {
            throw new NativeProtocolException(
                    "unsupported native response version " + version + "; expected protocol v1");
        }
        int flags = data.readUnsignedByte();
        long laneId = Integer.toUnsignedLong(data.readInt());
        int opcode = data.readUnsignedShort();
        long requestId = data.readLong();
        long bodyLength = Integer.toUnsignedLong(data.readInt());
        int currentLimit = maxFrameBytes.getAsInt();
        if (currentLimit < 0) {
            throw new IllegalArgumentException("maxFrameBytes must be non-negative");
        }
        if (bodyLength > currentLimit || bodyLength > Integer.MAX_VALUE) {
            throw new NativeProtocolException("native response frame exceeds max_response_bytes");
        }
        byte[] body = new byte[(int) bodyLength];
        data.readFully(body);
        return new NativeFrame(new Identity(laneId, opcode, requestId), flags, body);
    }
}
