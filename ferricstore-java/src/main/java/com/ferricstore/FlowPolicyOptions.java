package com.ferricstore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record FlowPolicyOptions(
        String state,
        RetryPolicy retry,
        Long retentionTtlMs,
        MaxActiveMs maxActiveMs,
        Boolean replace,
        Long expectedGeneration,
        List<String> indexedAttributes,
        boolean indexedAttributesPresent,
        String indexedStateMeta,
        FlowStateMode mode,
        Map<String, FlowStatePolicy> states) {
    public FlowPolicyOptions {
        indexedAttributes = ImmutableCopies.list(indexedAttributes);
        states = ImmutableCopies.map(states);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String state;
        private RetryPolicy retry;
        private Long retentionTtlMs;
        private MaxActiveMs maxActiveMs;
        private Boolean replace;
        private Long expectedGeneration;
        private final List<String> indexedAttributes = new ArrayList<>();
        private boolean indexedAttributesPresent;
        private String indexedStateMeta;
        private FlowStateMode mode;
        private final Map<String, FlowStatePolicy> states = new LinkedHashMap<>();

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

        public Builder replace(boolean value) {
            replace = value;
            return this;
        }

        public Builder expectedGeneration(long value) {
            if (value < 0) {
                throw new IllegalArgumentException("expectedGeneration must be non-negative");
            }
            expectedGeneration = value;
            return this;
        }

        public Builder indexedAttribute(String value) {
            indexedAttributesPresent = true;
            indexedAttributes.add(value);
            return this;
        }

        public Builder indexedAttributes(List<String> values) {
            indexedAttributesPresent = true;
            indexedAttributes.clear();
            indexedAttributes.addAll(values);
            return this;
        }

        public Builder indexedStateMeta(String value) {
            indexedStateMeta = value;
            return this;
        }

        /** Sets the mode for the legacy single-state {@link #state(String)} builder form. */
        public Builder mode(FlowStateMode value) {
            mode = value;
            return this;
        }

        public Builder statePolicy(String state, FlowStatePolicy policy) {
            FlowValidation.requireText(state, "policy state");
            states.put(state, Objects.requireNonNull(policy, "policy"));
            return this;
        }

        public Builder statePolicies(Map<String, FlowStatePolicy> values) {
            states.clear();
            values.forEach(this::statePolicy);
            return this;
        }

        public FlowPolicyOptions build() {
            return new FlowPolicyOptions(
                    state,
                    retry,
                    retentionTtlMs,
                    maxActiveMs,
                    replace,
                    expectedGeneration,
                    indexedAttributes,
                    indexedAttributesPresent,
                    indexedStateMeta,
                    mode,
                    states);
        }
    }
}
