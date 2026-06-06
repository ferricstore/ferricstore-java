package com.ferricstore;

public record RetryPolicy(Integer maxRetries, String backoff, Long baseMs, Long maxMs, Integer jitterPct, String exhaustedTo) {
}
