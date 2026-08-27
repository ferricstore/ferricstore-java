package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
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
            for (Future<R> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FerricStoreException("worker interrupted", e);
        } catch (ExecutionException e) {
            throw new FerricStoreException("worker task failed", e);
        } catch (CancellationException e) {
            throw new FerricStoreException("worker task cancelled", e);
        } finally {
            executorLease.completeBatch(futures);
        }
    }
}
