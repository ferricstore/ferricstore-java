package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

final class WorkerExecutors {
    static final int DEFAULT_BATCH_SIZE = 100;

    private WorkerExecutors() {}

    static void requirePositive(String name, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be >= 1");
        }
    }

    static <T, R> List<R> run(
            List<T> items,
            int concurrency,
            WorkerExecutorLease executorLease,
            Function<T, R> task) {
        requirePositive("concurrency", concurrency);
        if (items.isEmpty()) {
            return List.of();
        }
        if (executorLease.executor() == null) {
            return items.stream().map(task).toList();
        }

        List<Future<R>> futures = List.of();
        try {
            Semaphore permits = new Semaphore(concurrency);
            List<Callable<R>> calls = new ArrayList<>(items.size());
            for (T item : items) {
                calls.add(
                        () -> {
                            boolean acquired = false;
                            try {
                                permits.acquire();
                                acquired = true;
                                return task.apply(item);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new FerricStoreException("worker interrupted", e);
                            } finally {
                                if (acquired) {
                                    permits.release();
                                }
                            }
                        });
            }
            futures = executorLease.submitAll(calls);
            List<R> results = new ArrayList<>(futures.size());
            Throwable firstFailure = null;
            for (Future<R> future : futures) {
                try {
                    results.add(future.get());
                } catch (ExecutionException | java.util.concurrent.CancellationException failure) {
                    if (firstFailure == null) {
                        firstFailure =
                                failure instanceof ExecutionException execution
                                        ? AsyncFutures.unwrap(execution)
                                        : failure;
                    }
                }
            }
            executorLease.awaitTaskBodies(futures);
            if (firstFailure != null) {
                throw propagate(firstFailure);
            }
            return results;
        } catch (InterruptedException e) {
            futures.forEach(future -> future.cancel(true));
            boolean interruptedAgain = false;
            while (true) {
                try {
                    executorLease.awaitTaskBodies(futures);
                    break;
                } catch (InterruptedException ignored) {
                    interruptedAgain = true;
                }
            }
            Thread.currentThread().interrupt();
            if (interruptedAgain) {
                e.addSuppressed(new InterruptedException("interrupted again while draining tasks"));
            }
            throw new FerricStoreException("worker interrupted", e);
        } finally {
            executorLease.completeBatch(futures);
        }
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new FerricStoreException("worker task failed", failure);
    }
}
