package com.ferricstore;

final class NativeRetryPolicy {
    private static final int MAX_SERVER_DIRECTED_RETRIES = 2;
    private static final long MAX_RETRY_AFTER_MS = 60_000;

    private NativeRetryPolicy() {}

    static boolean shouldRetry(NativeServerException error, int attemptsAlreadyMade) {
        if (attemptsAlreadyMade >= MAX_SERVER_DIRECTED_RETRIES) {
            return false;
        }
        if (error.status() != NativeProtocol.STATUS_BUSY
                && error.status() != NativeProtocol.STATUS_REROUTE) {
            return false;
        }
        return Boolean.TRUE.equals(error.retryable()) && Boolean.TRUE.equals(error.safeToRetry());
    }

    static long retryAfterMs(NativeServerException error) {
        Long value = error.retryAfterMs();
        return value == null ? 0 : Math.min(Math.max(value, 0), MAX_RETRY_AFTER_MS);
    }
}
