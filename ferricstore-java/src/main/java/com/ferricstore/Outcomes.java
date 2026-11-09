package com.ferricstore;

import java.util.List;

public final class Outcomes {
    private Outcomes() {}

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

    /**
     * Creates the children and parks the parent in {@code waitState} until all of them finish. Use
     * {@link WaitForChildrenOutcome#builder} for a group id, {@code "any"}, or failure policies.
     */
    public static WaitForChildrenOutcome waitForChildren(
            List<ChildSpec> children, String waitState, String success, String failure) {
        return WaitForChildrenOutcome.builder(children, waitState, success, failure).build();
    }
}
