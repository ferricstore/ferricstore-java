package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;

final class FlowValidation {
    static final long MAX_EXACT_INTEGER = 9_007_199_254_740_991L;

    private FlowValidation() {}

    static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    static void requireFencingToken(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("flow fencing token must be non-negative");
        }
    }

    static void requirePositive(long value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    static void requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
    }

    static void requireOptionalPositive(Long value, String field) {
        if (value != null) {
            requirePositive(value, field);
        }
    }

    static void requireOptionalNonNegative(Long value, String field) {
        if (value != null) {
            requireNonNegative(value, field);
        }
    }

    static void requireOptionalExactNonNegative(Long value, String field) {
        requireOptionalNonNegative(value, field);
        if (value != null && value > MAX_EXACT_INTEGER) {
            throw new IllegalArgumentException(field + " exceeds the exact integer range");
        }
    }

    static void requireOptionalExactPositive(Long value, String field) {
        requireOptionalPositive(value, field);
        if (value != null && value > MAX_EXACT_INTEGER) {
            throw new IllegalArgumentException(field + " exceeds the exact integer range");
        }
    }

    static void requireTextList(List<String> values, String field, boolean allowEmpty) {
        if (values == null) {
            return;
        }
        if (!allowEmpty && values.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        for (String value : values) {
            requireText(value, field + " item");
        }
    }

    static void requireUniqueTextList(
            List<String> values, String field, int maximumItems, int maximumBytes) {
        requireTextList(values, field, false);
        if (values.size() > maximumItems) {
            throw new IllegalArgumentException(field + " cannot exceed " + maximumItems + " items");
        }
        if (new HashSet<>(values).size() != values.size()) {
            throw new IllegalArgumentException(field + " must contain unique values");
        }
        for (String value : values) {
            if (value.getBytes(StandardCharsets.UTF_8).length > maximumBytes) {
                throw new IllegalArgumentException(
                        field + " values cannot exceed " + maximumBytes + " bytes");
            }
        }
    }
}
