package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class FlowQueryAwaitTest {
    @Test
    void containingWaitsPastOtherMatchesForTheExpectedFlow() {
        FlowRecord other = record("other");
        FlowRecord expected = record("expected");
        AtomicInteger calls = new AtomicInteger();

        List<FlowRecord> result =
                FlowQueryAwait.containing(
                        () ->
                                calls.incrementAndGet() < 3
                                        ? List.of(other)
                                        : List.of(other, expected),
                        "expected",
                        5_000);

        assertTrue(result.contains(expected), "result should include the expected Flow");
        assertEquals(3, calls.get());
    }

    @Test
    void containingReturnsTheLastResultAtTheDeadline() {
        FlowRecord other = record("other");

        assertEquals(
                List.of(other), FlowQueryAwait.containing(() -> List.of(other), "expected", 50));
    }

    @Test
    void nonEmptyReturnsTheFirstVisibleMatch() {
        FlowRecord other = record("other");
        AtomicInteger calls = new AtomicInteger();

        List<FlowRecord> result =
                FlowQueryAwait.nonEmpty(
                        () -> calls.incrementAndGet() < 2 ? List.of() : List.of(other), 5_000);

        assertEquals(List.of(other), result);
        assertEquals(2, calls.get());
    }

    private static FlowRecord record(String id) {
        return new FlowRecord(
                id,
                "type",
                "queued",
                "partition",
                null,
                null,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
