package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

final class AsyncFuturesTest {
    @Test
    void composeCompletesFromTheMappedStageAndPropagatesItsFailure() {
        assertEquals(
                "mapped-value",
                AsyncFutures.compose(
                                CompletableFuture.completedFuture("value"),
                                value -> CompletableFuture.completedFuture("mapped-" + value))
                        .join());

        CompletableFuture<String> failed =
                AsyncFutures.compose(
                        CompletableFuture.completedFuture("value"),
                        ignored ->
                                CompletableFuture.failedFuture(new IllegalStateException("bad")));
        CompletionException failure = assertThrows(CompletionException.class, failed::join);
        assertTrue(failure.getCause() instanceof IllegalStateException);
    }

    @Test
    void cancellingComposeCancelsWhicheverStageIsActive() {
        AtomicBoolean mapped = new AtomicBoolean();
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> beforeMapping =
                AsyncFutures.compose(
                        source,
                        value -> {
                            mapped.set(true);
                            return CompletableFuture.completedFuture(value);
                        });

        assertTrue(beforeMapping.cancel(false));
        assertTrue(source.isCancelled());
        assertFalse(mapped.get());

        CompletableFuture<String> active = new CompletableFuture<>();
        CompletableFuture<String> afterMapping =
                AsyncFutures.compose(CompletableFuture.completedFuture("value"), ignored -> active);
        assertTrue(afterMapping.cancel(false));
        assertTrue(active.isCancelled());
    }
}
