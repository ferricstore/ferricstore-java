package com.ferricstore;

public record CancelOptions(
        String id,
        long fencingToken,
        String leaseToken,
        String partitionKey,
        Object reason,
        Long ttlMs,
        long nowMs,
        boolean returnRecord) {
    public static Builder builder(String id, long fencingToken) {
        return new Builder(id, fencingToken);
    }

    public static final class Builder {
        private final String id;
        private final long fencingToken;
        private String leaseToken;
        private String partitionKey;
        private Object reason;
        private Long ttlMs;
        private long nowMs;
        private boolean returnRecord;

        private Builder(String id, long fencingToken) {
            this.id = id;
            this.fencingToken = fencingToken;
        }

        public Builder leaseToken(String value) {
            this.leaseToken = value;
            return this;
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            return this;
        }

        public Builder reason(Object value) {
            this.reason = value;
            return this;
        }

        public Builder ttlMs(long value) {
            this.ttlMs = value;
            return this;
        }

        public Builder nowMs(long value) {
            this.nowMs = value;
            return this;
        }

        public Builder returnRecord(boolean value) {
            this.returnRecord = value;
            return this;
        }

        public CancelOptions build() {
            return new CancelOptions(
                    id, fencingToken, leaseToken, partitionKey, reason, ttlMs, nowMs, returnRecord);
        }
    }
}
