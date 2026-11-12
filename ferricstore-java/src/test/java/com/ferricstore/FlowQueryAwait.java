package com.ferricstore;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Polls a Flow query until its lagging projection shows what a test expects. */
final class FlowQueryAwait {
    private FlowQueryAwait() {}

    static List<FlowRecord> containing(
            Supplier<List<FlowRecord>> records, String expectedId, long timeoutMs) {
        // Other matching Flows can become visible before the expected one.
        return until(
                records,
                current -> current.stream().anyMatch(record -> record.id().equals(expectedId)),
                timeoutMs);
    }

    static List<FlowRecord> nonEmpty(Supplier<List<FlowRecord>> records, long timeoutMs) {
        return until(records, current -> !current.isEmpty(), timeoutMs);
    }

    private static List<FlowRecord> until(
            Supplier<List<FlowRecord>> records, Predicate<List<FlowRecord>> done, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        List<FlowRecord> current;
        do {
            current = records.get();
            if (done.test(current)) {
                return current;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(
                        "interrupted while waiting for Flow query projection", error);
            }
        } while (System.nanoTime() < deadline);
        return current;
    }
}
