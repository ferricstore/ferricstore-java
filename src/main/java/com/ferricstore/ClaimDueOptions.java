package com.ferricstore;

public record ClaimDueOptions(
    String type,
    String state,
    String worker,
    String partitionKey,
    long leaseMs,
    int limit,
    long nowMs,
    Boolean reclaimExpired,
    Long reclaimRatio
) {
    public static Builder builder(String type, String worker) {
        return new Builder(type, worker);
    }

    public static final class Builder {
        private final String type;
        private final String worker;
        private String state = "queued";
        private String partitionKey;
        private long leaseMs = 30_000;
        private int limit = 1;
        private long nowMs;
        private Boolean reclaimExpired;
        private Long reclaimRatio;

        private Builder(String type, String worker) {
            this.type = type;
            this.worker = worker;
        }

        public Builder state(String value) { this.state = value; return this; }
        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder leaseMs(long value) { this.leaseMs = value; return this; }
        public Builder limit(int value) { this.limit = value; return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }
        public Builder reclaimExpired(boolean value) { this.reclaimExpired = value; return this; }
        public Builder reclaimRatio(long value) { this.reclaimRatio = value; return this; }

        public ClaimDueOptions build() {
            return new ClaimDueOptions(type, state, worker, partitionKey, leaseMs, limit,
                nowMs, reclaimExpired, reclaimRatio);
        }
    }
}
