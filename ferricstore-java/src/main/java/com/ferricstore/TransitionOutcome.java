package com.ferricstore;

public record TransitionOutcome(String toState, Object payload, Long runAtMs, Long priority)
        implements Outcome {}
