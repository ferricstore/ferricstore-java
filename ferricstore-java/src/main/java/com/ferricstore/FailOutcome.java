package com.ferricstore;

public record FailOutcome(Object error, Object payload, Long ttlMs) implements Outcome {
}
