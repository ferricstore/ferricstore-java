package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

final class WorkerExecutors {
    static final int DEFAULT_BATCH_SIZE = 100;

    private WorkerExecutors() {
    }

    static void requirePositive(String name, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be >= 1");
        }
    }

    static <T, R> List<R> run(List<T> items, int concurrency, boolean virtualThreads, ExecutorService executor, Function<T, R> task) {
        requirePositive("concurrency", concurrency);
        if (items.isEmpty()) {
            return List.of();
        }
        if (concurrency == 1) {
            return items.stream().map(task).toList();
        }

        ExecutorService service = executor;
        boolean ownsExecutor = service == null;
        if (service == null) {
            service = virtualThreads ? Executors.newVirtualThreadPerTaskExecutor() : Executors.newFixedThreadPool(concurrency);
        }

        try {
            Semaphore permits = new Semaphore(concurrency);
            List<Callable<R>> calls = new ArrayList<>(items.size());
            for (T item : items) {
                calls.add(() -> {
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
            List<Future<R>> futures = service.invokeAll(calls);
            List<R> results = new ArrayList<>(futures.size());
            for (Future<R> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FerricStoreException("worker interrupted", e);
        } catch (ExecutionException e) {
            throw new FerricStoreException("worker task failed", e.getCause());
        } finally {
            if (ownsExecutor) {
                service.shutdown();
            }
        }
    }
}
