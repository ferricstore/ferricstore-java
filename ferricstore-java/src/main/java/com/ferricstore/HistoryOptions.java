package com.ferricstore;

/** Complete option surface for {@code FLOW.HISTORY}. */
public record HistoryOptions(
        String partitionKey,
        int count,
        String fromEvent,
        String toEvent,
        Long fromMs,
        Long toMs,
        Long fromVersion,
        Long toVersion,
        Boolean reverse,
        String event,
        String worker,
        Boolean includeCold,
        Boolean consistentProjection,
        Boolean values,
        Long payloadMaxBytes) {
    public HistoryOptions {
        FlowValidation.requirePositive(count, "history count");
        FlowValidation.requireOptionalNonNegative(fromMs, "history fromMs");
        FlowValidation.requireOptionalNonNegative(toMs, "history toMs");
        FlowValidation.requireOptionalNonNegative(fromVersion, "history fromVersion");
        FlowValidation.requireOptionalNonNegative(toVersion, "history toVersion");
        FlowValidation.requireOptionalNonNegative(payloadMaxBytes, "history payloadMaxBytes");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String partitionKey;
        private int count = 100;
        private String fromEvent;
        private String toEvent;
        private Long fromMs;
        private Long toMs;
        private Long fromVersion;
        private Long toVersion;
        private Boolean reverse;
        private String event;
        private String worker;
        private Boolean includeCold;
        private Boolean consistentProjection;
        private Boolean values;
        private Long payloadMaxBytes;

        public Builder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public Builder count(int value) {
            count = value;
            return this;
        }

        public Builder fromEvent(String value) {
            fromEvent = value;
            return this;
        }

        public Builder toEvent(String value) {
            toEvent = value;
            return this;
        }

        public Builder fromMs(long value) {
            fromMs = value;
            return this;
        }

        public Builder toMs(long value) {
            toMs = value;
            return this;
        }

        public Builder fromVersion(long value) {
            fromVersion = value;
            return this;
        }

        public Builder toVersion(long value) {
            toVersion = value;
            return this;
        }

        public Builder reverse(boolean value) {
            reverse = value;
            return this;
        }

        public Builder event(String value) {
            event = value;
            return this;
        }

        public Builder worker(String value) {
            worker = value;
            return this;
        }

        public Builder includeCold(boolean value) {
            includeCold = value;
            return this;
        }

        public Builder consistentProjection(boolean value) {
            consistentProjection = value;
            return this;
        }

        public Builder values(boolean value) {
            values = value;
            return this;
        }

        public Builder payloadMaxBytes(long value) {
            payloadMaxBytes = value;
            return this;
        }

        public HistoryOptions build() {
            return new HistoryOptions(
                    partitionKey,
                    count,
                    fromEvent,
                    toEvent,
                    fromMs,
                    toMs,
                    fromVersion,
                    toVersion,
                    reverse,
                    event,
                    worker,
                    includeCold,
                    consistentProjection,
                    values,
                    payloadMaxBytes);
        }
    }
}
