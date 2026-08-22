package com.ferricstore;

final class FlowValidation {
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
}
