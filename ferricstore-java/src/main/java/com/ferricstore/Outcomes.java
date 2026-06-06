package com.ferricstore;

public final class Outcomes {
    private Outcomes() {
    }

    public static TransitionOutcome transition(String toState) {
        return new TransitionOutcome(toState, null, null, null);
    }

    public static TransitionOutcome transition(String toState, Object payload) {
        return new TransitionOutcome(toState, payload, null, null);
    }

    public static CompleteOutcome complete(Object result) {
        return new CompleteOutcome(result, null, null);
    }

    public static RetryOutcome retry(Object error) {
        return new RetryOutcome(error, null, null);
    }

    public static FailOutcome fail(Object error) {
        return new FailOutcome(error, null, null);
    }
}
