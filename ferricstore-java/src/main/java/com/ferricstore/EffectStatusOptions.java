package com.ferricstore;

public record EffectStatusOptions(
        String partitionKey,
        String leaseToken,
        Long fencingToken,
        String externalId,
        String error,
        String reason,
        Long latencyMs,
        Long nowMs) {
    public EffectStatusOptions {
        if (fencingToken != null) {
            FlowValidation.requireFencingToken(fencingToken);
        }
        if (leaseToken != null) {
            FlowValidation.requireText(leaseToken, "flow lease token");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String partitionKey;
        private String leaseToken;
        private Long fencingToken;
        private String externalId;
        private String error;
        private String reason;
        private Long latencyMs;
        private Long nowMs;

        public Builder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public Builder lease(String token, long fencing) {
            leaseToken = token;
            fencingToken = fencing;
            return this;
        }

        public Builder externalId(String value) {
            externalId = value;
            return this;
        }

        public Builder error(String value) {
            error = value;
            return this;
        }

        public Builder reason(String value) {
            reason = value;
            return this;
        }

        public Builder latencyMs(long value) {
            latencyMs = value;
            return this;
        }

        public Builder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public EffectStatusOptions build() {
            return new EffectStatusOptions(
                    partitionKey,
                    leaseToken,
                    fencingToken,
                    externalId,
                    error,
                    reason,
                    latencyMs,
                    nowMs);
        }
    }
}
