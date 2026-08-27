package com.ferricstore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Complete option surface for {@code FLOW.SIGNAL}. */
public record SignalOptions(
        String signal,
        String partitionKey,
        String idempotencyKey,
        List<String> ifStates,
        String transitionTo,
        Long runAtMs,
        Long nowMs,
        Map<String, ?> values,
        Map<String, String> valueRefs,
        List<String> dropValues,
        List<String> overrideValues) {
    public SignalOptions {
        FlowValidation.requireText(signal, "signal");
        ifStates = ImmutableCopies.list(ifStates);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        dropValues = ImmutableCopies.list(dropValues);
        overrideValues = ImmutableCopies.list(overrideValues);
    }

    public static Builder builder(String signal) {
        return new Builder(signal);
    }

    public static final class Builder {
        private final String signal;
        private String partitionKey;
        private String idempotencyKey;
        private final List<String> ifStates = new ArrayList<>();
        private String transitionTo;
        private Long runAtMs;
        private Long nowMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private final List<String> dropValues = new ArrayList<>();
        private final List<String> overrideValues = new ArrayList<>();

        private Builder(String signal) {
            this.signal = signal;
        }

        public Builder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public Builder idempotencyKey(String value) {
            idempotencyKey = value;
            return this;
        }

        public Builder ifState(String value) {
            ifStates.add(value);
            return this;
        }

        public Builder ifStates(List<String> values) {
            ifStates.addAll(values);
            return this;
        }

        public Builder transitionTo(String value) {
            transitionTo = value;
            return this;
        }

        public Builder runAtMs(long value) {
            runAtMs = value;
            return this;
        }

        public Builder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public Builder value(String name, Object value) {
            values.put(name, value);
            return this;
        }

        public Builder valueRef(String name, String value) {
            valueRefs.put(name, value);
            return this;
        }

        public Builder dropValue(String value) {
            dropValues.add(value);
            return this;
        }

        public Builder overrideValue(String value) {
            overrideValues.add(value);
            return this;
        }

        public SignalOptions build() {
            return new SignalOptions(
                    signal,
                    partitionKey,
                    idempotencyKey,
                    ifStates,
                    transitionTo,
                    runAtMs,
                    nowMs,
                    values,
                    valueRefs,
                    dropValues,
                    overrideValues);
        }
    }
}
