package com.ferricstore;

public record RetryOutcome(Object error, Object payload, Long runAtMs) implements Outcome {
}
