package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class WorkerExecutorLease implements AutoCloseable {
    private static final AtomicLong THREAD_SEQUENCE = new AtomicLong();

    private final ExecutorService executor;
    private final boolean owned;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object activeLock = new Object();
    private List<? extends Future<?>> activeBatch = List.of();

    private WorkerExecutorLease(ExecutorService executor, boolean owned) {
        this.executor = executor;
        this.owned = owned;
    }

    static WorkerExecutorLease create(
            int concurrency, boolean virtualThreads, ExecutorService suppliedExecutor) {
        WorkerExecutors.requirePositive("concurrency", concurrency);
        if (suppliedExecutor != null) {
            return new WorkerExecutorLease(suppliedExecutor, false);
        }
        if (virtualThreads) {
            return new WorkerExecutorLease(VirtualThreadSupport.newExecutor(), true);
        }
        if (concurrency == 1) {
            return new WorkerExecutorLease(null, false);
        }
        ThreadFactory factory =
                task -> new Thread(task, "ferricstore-worker-" + THREAD_SEQUENCE.incrementAndGet());
        return new WorkerExecutorLease(Executors.newFixedThreadPool(concurrency, factory), true);
    }

    ExecutorService executor() {
        return executor;
    }

    <T> List<Future<T>> submitAll(List<? extends Callable<T>> tasks) {
        if (executor == null) {
            throw new IllegalStateException("inline worker execution cannot submit tasks");
        }
        synchronized (activeLock) {
            ensureOpen();
            List<Future<T>> futures = new ArrayList<>(tasks.size());
            try {
                for (Callable<T> task : tasks) {
                    futures.add(executor.submit(task));
                }
            } catch (RejectedExecutionException e) {
                futures.forEach(future -> future.cancel(true));
                throw e;
            }
            activeBatch = futures;
            return futures;
        }
    }

    void completeBatch(List<? extends Future<?>> futures) {
        synchronized (activeLock) {
            if (activeBatch.equals(futures)) {
                activeBatch = List.of();
            }
        }
    }

    void cancelActive() {
        synchronized (activeLock) {
            closed.set(true);
            activeBatch.forEach(future -> future.cancel(true));
            activeBatch = List.of();
        }
        if (owned && executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (owned && executor != null) {
            executor.shutdown();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("worker session is closed");
        }
    }
}
