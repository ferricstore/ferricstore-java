package com.ferricstore;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

final class AsyncFutures {
    private AsyncFutures() {}

    static <T> T await(CompletableFuture<T> future, String interruptedMessage) {
        return await(future, error -> new FerricStoreException(interruptedMessage, error));
    }

    static <T> T await(
            CompletableFuture<T> future,
            Function<? super InterruptedException, ? extends RuntimeException> interruptedFailure) {
        try {
            return future.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            future.cancel(false);
            throw interruptedFailure.apply(error);
        } catch (ExecutionException error) {
            throw runtimeFailure(unwrap(error));
        }
    }

    static <T> CompletableFuture<T> failed(Throwable error) {
        return CompletableFuture.failedFuture(unwrap(error));
    }

    static <T> CompletableFuture<List<T>> sequence(
            List<? extends CompletableFuture<? extends T>> futures) {
        CompletableFuture<?>[] all = futures.toArray(CompletableFuture[]::new);
        CompletableFuture<List<T>> result =
                CompletableFuture.allOf(all)
                        .thenApply(
                                ignored ->
                                        futures.stream()
                                                .map(CompletableFuture::join)
                                                .map(value -> (T) value)
                                                .toList());
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        futures.forEach(future -> future.cancel(false));
                    }
                });
        return result;
    }

    static <S, T> CompletableFuture<T> map(
            CompletableFuture<S> source, Function<? super S, ? extends T> mapper) {
        CompletableFuture<T> result = new CompletableFuture<>();
        source.whenComplete(
                (value, failure) -> {
                    if (failure != null) {
                        result.completeExceptionally(unwrap(failure));
                        return;
                    }
                    try {
                        result.complete(mapper.apply(value));
                    } catch (RuntimeException error) {
                        result.completeExceptionally(error);
                    }
                });
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        source.cancel(false);
                    }
                });
        return result;
    }

    static <S, T> CompletableFuture<T> compose(
            CompletableFuture<S> source,
            Function<? super S, ? extends CompletableFuture<T>> mapper) {
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<T>> next = new AtomicReference<>();
        source.whenComplete(
                (value, failure) -> {
                    if (failure != null) {
                        result.completeExceptionally(unwrap(failure));
                        return;
                    }
                    CompletableFuture<T> mapped;
                    try {
                        mapped = mapper.apply(value);
                    } catch (RuntimeException error) {
                        result.completeExceptionally(error);
                        return;
                    }
                    next.set(mapped);
                    if (result.isCancelled()) {
                        mapped.cancel(false);
                        return;
                    }
                    mapped.whenComplete(
                            (mappedValue, mappedFailure) -> {
                                if (mappedFailure != null) {
                                    result.completeExceptionally(unwrap(mappedFailure));
                                } else {
                                    result.complete(mappedValue);
                                }
                            });
                });
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        source.cancel(false);
                        CompletableFuture<T> mapped = next.get();
                        if (mapped != null) {
                            mapped.cancel(false);
                        }
                    }
                });
        return result;
    }

    static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static RuntimeException runtimeFailure(Throwable error) {
        if (error instanceof RuntimeException runtime) {
            return runtime;
        }
        return new FerricStoreException("asynchronous FerricStore request failed", error);
    }
}
