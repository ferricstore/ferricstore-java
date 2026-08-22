package com.ferricstore;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

final class NativeResponseAssembler {
    record Assembled(NativeFrame.Identity identity, int flags, byte[] body) {
    }

    private final Map<NativeFrame.Identity, Pending> pending = new HashMap<>();
    private int maxResponseBytes;
    private final int maxChunks;

    NativeResponseAssembler(int maxResponseBytes, int maxChunks) {
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
        if (maxChunks <= 0) {
            throw new IllegalArgumentException("maxChunks must be positive");
        }
        this.maxResponseBytes = maxResponseBytes;
        this.maxChunks = maxChunks;
    }

    synchronized Assembled add(NativeFrame.Identity identity, int flags, byte[] chunk) {
        boolean more = (flags & NativeProtocol.FLAG_MORE_CHUNKS) != 0;
        int logicalFlags = flags & ~NativeProtocol.FLAG_MORE_CHUNKS;
        Pending current = pending.get(identity);
        if (current == null && !more) {
            checkSize(chunk.length);
            return new Assembled(identity, logicalFlags, chunk);
        }
        if (current == null) {
            current = new Pending(logicalFlags, Math.min(chunk.length, maxResponseBytes));
            pending.put(identity, current);
        }
        try {
            current.append(chunk, logicalFlags, maxResponseBytes, maxChunks);
        } catch (RuntimeException error) {
            pending.remove(identity);
            throw error;
        }
        if (more) {
            return null;
        }
        pending.remove(identity);
        return new Assembled(identity, current.flags, current.output.toByteArray());
    }

    synchronized void reconfigure(int negotiatedMaxResponseBytes) {
        if (!pending.isEmpty()) {
            throw new NativeProtocolException(
                    "cannot change max_response_bytes while response chunks are pending");
        }
        if (negotiatedMaxResponseBytes <= 0) {
            throw new IllegalArgumentException("negotiatedMaxResponseBytes must be positive");
        }
        maxResponseBytes = negotiatedMaxResponseBytes;
    }

    synchronized int pendingCount() {
        return pending.size();
    }

    synchronized void clear() {
        pending.clear();
    }

    private void checkSize(int bytes) {
        if (bytes > maxResponseBytes) {
            throw new NativeProtocolException(
                    "native response exceeds negotiated max_response_bytes");
        }
    }

    private static final class Pending {
        private final ByteArrayOutputStream output;
        private int flags;
        private int chunks;

        private Pending(int flags, int initialCapacity) {
            this.flags = flags;
            this.output = new ByteArrayOutputStream(initialCapacity);
        }

        private void append(byte[] chunk, int chunkFlags, int maxBytes, int maxChunks) {
            chunks++;
            if (chunks > maxChunks) {
                throw new NativeProtocolException(
                        "native response exceeds max_response_chunks");
            }
            if (chunk.length > maxBytes - output.size()) {
                throw new NativeProtocolException(
                        "native response exceeds negotiated max_response_bytes");
            }
            output.writeBytes(chunk);
            flags |= chunkFlags;
        }
    }
}
