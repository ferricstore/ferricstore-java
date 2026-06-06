package com.ferricstore;

public record FailOptions(String id, String leaseToken, long fencingToken, String partitionKey, Object error, Object payload, Long ttlMs, long nowMs, boolean returnRecord) {
    public static Builder builder(String id, String leaseToken, long fencingToken) { return new Builder(id, leaseToken, fencingToken); }
    public static final class Builder {
        private final String id; private final String leaseToken; private final long fencingToken;
        private String partitionKey; private Object error; private Object payload; private Long ttlMs; private long nowMs; private boolean returnRecord;
        private Builder(String id, String leaseToken, long fencingToken) { this.id = id; this.leaseToken = leaseToken; this.fencingToken = fencingToken; }
        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder error(Object value) { this.error = value; return this; }
        public Builder payload(Object value) { this.payload = value; return this; }
        public Builder ttlMs(long value) { this.ttlMs = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder returnRecord(boolean value) { this.returnRecord = value; return this; }
        public FailOptions build() { return new FailOptions(id, leaseToken, fencingToken, partitionKey, error, payload, ttlMs, nowMs, returnRecord); }
    }
}
