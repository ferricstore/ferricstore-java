package com.ferricstore;

import java.util.Objects;

/**
 * The refreshed claim and codec-normalized result of one durable workflow step.
 *
 * @param job refreshed workflow claim that must be used for the next mutation
 * @param result stored result, decoded identically on first execution and replay
 * @param replayed {@code true} when the closure was skipped and a committed result was returned
 * @param <T> result type
 */
public record DurableStepResult<T>(ClaimedItem job, T result, boolean replayed) {
    public DurableStepResult {
        Objects.requireNonNull(job, "job");
    }
}
