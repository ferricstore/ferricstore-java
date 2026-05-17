package com.ferricstore;

public record CompleteOptions(
    String id,
    String leaseToken,
    long fencingToken,
    String partitionKey,
    byte[] result,
    byte[] payload,
    Long ttlMs,
    long nowMs,
    boolean returnRecord
) {
    public static Builder builder(String id, String leaseToken, long fencingToken) {
        return new Builder(id, leaseToken, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final String leaseToken;
        private final long fencingToken;
        private String partitionKey;
        private byte[] result;
        private byte[] payload;
        private Long ttlMs;
        private long nowMs;
        private boolean returnRecord;

        private Builder(String id, String leaseToken, long fencingToken) {
            this.id = id;
            this.leaseToken = leaseToken;
            this.fencingToken = fencingToken;
        }

        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder result(byte[] value) { this.result = value; return this; }
        public Builder payload(byte[] value) { this.payload = value; return this; }
        public Builder ttlMs(long value) { this.ttlMs = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder returnRecord(boolean value) { this.returnRecord = value; return this; }

        public CompleteOptions build() {
            return new CompleteOptions(id, leaseToken, fencingToken, partitionKey, result,
                payload, ttlMs, nowMs, returnRecord);
        }
    }
}
