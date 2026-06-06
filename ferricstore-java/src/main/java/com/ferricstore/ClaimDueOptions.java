package com.ferricstore;

import java.util.ArrayList;
import java.util.List;

public record ClaimDueOptions(
        String type,
        String state,
        List<String> states,
        String worker,
        String partitionKey,
        List<String> partitionKeys,
        long leaseMs,
        int limit,
        long nowMs,
        Long blockMs,
        Long priority,
        Boolean reclaimExpired,
        Long reclaimRatio,
        Boolean payload,
        Long payloadMaxBytes,
        List<String> values,
        Long valueMaxBytes,
        boolean jobOnly,
        boolean includeState) {
    public ClaimDueOptions {
        states = ImmutableCopies.list(states);
        partitionKeys = ImmutableCopies.list(partitionKeys);
        values = ImmutableCopies.list(values);
    }

    public static Builder builder(String type, String worker) {
        return new Builder(type, worker);
    }

    public static final class Builder {
        private final String type;
        private final String worker;
        private String state = "queued";
        private final List<String> states = new ArrayList<>();
        private String partitionKey;
        private final List<String> partitionKeys = new ArrayList<>();
        private long leaseMs = 30_000;
        private int limit = 1;
        private long nowMs;
        private Long blockMs;
        private Long priority;
        private Boolean reclaimExpired;
        private Long reclaimRatio;
        private Boolean payload;
        private Long payloadMaxBytes;
        private final List<String> values = new ArrayList<>();
        private Long valueMaxBytes;
        private boolean jobOnly;
        private boolean includeState;

        private Builder(String type, String worker) {
            this.type = type;
            this.worker = worker;
        }

        public Builder state(String value) {
            this.state = value;
            this.states.clear();
            return this;
        }

        public Builder states(List<String> values) {
            this.state = null;
            this.states.clear();
            this.states.addAll(values);
            return this;
        }

        public Builder partitionKey(String value) {
            this.partitionKey = value;
            this.partitionKeys.clear();
            return this;
        }

        public Builder partitionKeys(List<String> values) {
            this.partitionKey = null;
            this.partitionKeys.clear();
            this.partitionKeys.addAll(values);
            return this;
        }

        public Builder leaseMs(long value) {
            this.leaseMs = value;
            return this;
        }

        public Builder limit(int value) {
            this.limit = value;
            return this;
        }

        public Builder nowMs(long value) {
            this.nowMs = value;
            return this;
        }

        public Builder blockMs(long value) {
            this.blockMs = value;
            return this;
        }

        public Builder priority(long value) {
            this.priority = value;
            return this;
        }

        public Builder reclaimExpired(boolean value) {
            this.reclaimExpired = value;
            return this;
        }

        public Builder reclaimRatio(long value) {
            this.reclaimRatio = value;
            return this;
        }

        public Builder payload(boolean value) {
            this.payload = value;
            return this;
        }

        public Builder payloadMaxBytes(long value) {
            this.payloadMaxBytes = value;
            return this;
        }

        public Builder value(String name) {
            this.values.add(name);
            return this;
        }

        public Builder values(List<String> names) {
            this.values.clear();
            this.values.addAll(names);
            return this;
        }

        public Builder valueMaxBytes(long value) {
            this.valueMaxBytes = value;
            return this;
        }

        public Builder jobOnly(boolean value) {
            this.jobOnly = value;
            return this;
        }

        public Builder includeState(boolean value) {
            this.includeState = value;
            return this;
        }

        public ClaimDueOptions build() {
            return new ClaimDueOptions(
                    type,
                    state,
                    List.copyOf(states),
                    worker,
                    partitionKey,
                    List.copyOf(partitionKeys),
                    leaseMs,
                    limit,
                    nowMs,
                    blockMs,
                    priority,
                    reclaimExpired,
                    reclaimRatio,
                    payload,
                    payloadMaxBytes,
                    List.copyOf(values),
                    valueMaxBytes,
                    jobOnly,
                    includeState);
        }
    }
}
