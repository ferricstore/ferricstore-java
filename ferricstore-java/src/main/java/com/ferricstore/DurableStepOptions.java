package com.ferricstore;

/**
 * Optional timing controls shared by {@code advance} and durable {@code step}.
 *
 * @param leaseMs duration of the renewed claim in milliseconds
 * @param nowMs optional time override used by deterministic tests; {@code null} uses the client
 *     JVM's current wall-clock time
 */
public record DurableStepOptions(long leaseMs, Long nowMs) {
    private static final long DEFAULT_LEASE_MS = 30_000;

    public DurableStepOptions {
        FlowValidation.requirePositive(leaseMs, "leaseMs");
        FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
    }

    /** Returns the production defaults: a 30-second lease and current client wall-clock time. */
    public static DurableStepOptions defaults() {
        return new DurableStepOptions(DEFAULT_LEASE_MS, null);
    }

    /** Returns a builder initialized with the production defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builds immutable durable-step timing options. */
    public static final class Builder {
        private long leaseMs = DEFAULT_LEASE_MS;
        private Long nowMs;

        private Builder() {}

        /** Sets the renewed claim duration in milliseconds. */
        public Builder leaseMs(long value) {
            leaseMs = value;
            return this;
        }

        /** Sets an explicit timestamp in milliseconds for deterministic execution. */
        public Builder nowMs(long value) {
            nowMs = value;
            return this;
        }

        /** Creates the validated options. */
        public DurableStepOptions build() {
            return new DurableStepOptions(leaseMs, nowMs);
        }
    }
}
