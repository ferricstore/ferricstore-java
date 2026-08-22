package com.ferricstore;

public record FlowPolicyOptions(
        String state, RetryPolicy retry, Long retentionTtlMs, MaxActiveMs maxActiveMs) {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String state;
        private RetryPolicy retry;
        private Long retentionTtlMs;
        private MaxActiveMs maxActiveMs;

        public Builder state(String value) {
            state = value;
            return this;
        }

        public Builder retry(RetryPolicy value) {
            retry = value;
            return this;
        }

        public Builder retentionTtlMs(long value) {
            retentionTtlMs = value;
            return this;
        }

        public Builder maxActiveMs(long value) {
            maxActiveMs = MaxActiveMs.of(value);
            return this;
        }

        public Builder maxActiveMs(MaxActiveMs value) {
            maxActiveMs = value;
            return this;
        }

        public FlowPolicyOptions build() {
            return new FlowPolicyOptions(state, retry, retentionTtlMs, maxActiveMs);
        }
    }
}
