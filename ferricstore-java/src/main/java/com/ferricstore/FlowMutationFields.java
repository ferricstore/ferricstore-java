package com.ferricstore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Attribute, state-metadata, and named-value controls shared by Flow mutations. */
public record FlowMutationFields(
        Map<String, ?> attributesMerge,
        List<String> attributesDelete,
        Map<String, ?> stateMeta,
        List<String> dropValues,
        List<String> overrideValues) {
    private static final FlowMutationFields EMPTY =
            new FlowMutationFields(Map.of(), List.of(), Map.of(), List.of(), List.of());

    public FlowMutationFields {
        attributesMerge = ImmutableCopies.map(attributesMerge);
        attributesDelete = ImmutableCopies.list(attributesDelete);
        stateMeta = ImmutableCopies.map(stateMeta);
        dropValues = ImmutableCopies.list(dropValues);
        overrideValues = ImmutableCopies.list(overrideValues);
    }

    public static FlowMutationFields empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, Object> attributesMerge = new LinkedHashMap<>();
        private final List<String> attributesDelete = new ArrayList<>();
        private final Map<String, Object> stateMeta = new LinkedHashMap<>();
        private final List<String> dropValues = new ArrayList<>();
        private final List<String> overrideValues = new ArrayList<>();

        private Builder() {}

        public Builder attributeMerge(String name, Object value) {
            attributesMerge.put(requireName(name, "attribute"), value);
            return this;
        }

        public Builder attributesMerge(Map<String, ?> values) {
            values.forEach(this::attributeMerge);
            return this;
        }

        public Builder attributeDelete(String name) {
            attributesDelete.add(requireName(name, "attribute"));
            return this;
        }

        public Builder attributesDelete(List<String> names) {
            names.forEach(this::attributeDelete);
            return this;
        }

        public Builder stateMeta(String state, Object value) {
            stateMeta.put(requireName(state, "state"), value);
            return this;
        }

        public Builder stateMeta(Map<String, ?> values) {
            values.forEach(this::stateMeta);
            return this;
        }

        public Builder dropValue(String name) {
            dropValues.add(requireName(name, "value"));
            return this;
        }

        public Builder dropValues(List<String> names) {
            names.forEach(this::dropValue);
            return this;
        }

        public Builder overrideValue(String name) {
            overrideValues.add(requireName(name, "value"));
            return this;
        }

        public Builder overrideValues(List<String> names) {
            names.forEach(this::overrideValue);
            return this;
        }

        public FlowMutationFields build() {
            return new FlowMutationFields(
                    attributesMerge, attributesDelete, stateMeta, dropValues, overrideValues);
        }
    }

    private static String requireName(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " name must not be blank");
        }
        return value;
    }
}
