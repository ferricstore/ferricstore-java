package com.ferricstore;

public record CompleteOutcome(Object result, Object payload, Long ttlMs) implements Outcome {}
