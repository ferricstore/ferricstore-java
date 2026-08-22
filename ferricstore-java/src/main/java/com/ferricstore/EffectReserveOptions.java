package com.ferricstore;

public record EffectReserveOptions(
        String partitionKey,
        String leaseToken,
        long fencingToken,
        String operationDigest,
        String idempotencyKey,
        String governanceScope,
        Long nowMs) {
    public EffectReserveOptions {
        FlowValidation.requireText(leaseToken, "flow lease token");
        FlowValidation.requireFencingToken(fencingToken);
        FlowValidation.requireText(operationDigest, "effect operation digest");
    }

    public static Builder builder(String leaseToken, long fencingToken) {
        return new Builder(leaseToken, fencingToken);
    }

    public static final class Builder {
        private final String leaseToken;
        private final long fencingToken;
        private String partitionKey;
        private String operationDigest;
        private String idempotencyKey;
        private String governanceScope;
        private Long nowMs;

        private Builder(String leaseToken, long fencingToken) {
            this.leaseToken = leaseToken;
            this.fencingToken = fencingToken;
        }

        public Builder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public Builder operationDigest(String value) {
            operationDigest = value;
            return this;
        }

        public Builder idempotencyKey(String value) {
            idempotencyKey = value;
            return this;
        }

        public Builder governanceScope(String value) {
            governanceScope = value;
            return this;
        }

        public Builder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public EffectReserveOptions build() {
            return new EffectReserveOptions(
                    partitionKey,
                    leaseToken,
                    fencingToken,
                    operationDigest,
                    idempotencyKey,
                    governanceScope,
                    nowMs);
        }
    }
}
