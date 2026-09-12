package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
        ThreadFactory factory =
                task -> new Thread(task, "ferricstore-worker-" + THREAD_SEQUENCE.incrementAndGet());
        return new WorkerExecutorLease(Executors.newFixedThreadPool(concurrency, factory), true);
    }

    ExecutorService executor() {
        return executor;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    <T> List<Future<T>> submitAll(List<? extends Callable<T>> tasks) {
        if (executor == null) {
            throw new IllegalStateException("inline worker execution cannot submit tasks");
        }
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        synchronized (activeLock) {
            ensureOpen();
            activeBatch = futures;
        }
        Thread submitter = Thread.currentThread();
        for (Callable<T> task : tasks) {
            TrackedFutureTask<T> future = new TrackedFutureTask<>(task, submitter);
            synchronized (activeLock) {
                if (closed.get()) {
                    futures.add(future);
                    future.reject(new IllegalStateException("worker session is closed"));
                    break;
                }
                futures.add(future);
            }
            try {
                executor.execute(future);
            } catch (RuntimeException failure) {
                future.reject(failure);
                break;
            }
        }
        return futures;
    }

    void awaitTaskBodies(List<? extends Future<?>> futures) throws InterruptedException {
        for (Future<?> future : futures) {
            if (future instanceof TrackedFutureTask<?> tracked) {
                tracked.awaitBody();
            }
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    void completeBatch(List<? extends Future<?>> futures) {
        synchronized (activeLock) {
            if (activeBatch == futures) {
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

    private static final class TrackedFutureTask<T> extends FutureTask<T> {
        private static final int NOT_STARTED = 0;
        private static final int RUNNING = 1;
        private static final int FINISHED = 2;

        private final Thread submitter;
        private final AtomicInteger lifecycle = new AtomicInteger(NOT_STARTED);
        private final CountDownLatch bodyFinished = new CountDownLatch(1);

        private TrackedFutureTask(Callable<T> task, Thread submitter) {
            super(task);
            this.submitter = submitter;
        }

        @Override
        public void run() {
            if (!lifecycle.compareAndSet(NOT_STARTED, RUNNING)) {
                return;
            }
            try {
                if (Thread.currentThread().equals(submitter)) {
                    super.setException(
                            new RejectedExecutionException(
                                    "worker executor ran application code inline"));
                    return;
                }
                super.run();
            } finally {
                lifecycle.set(FINISHED);
                bodyFinished.countDown();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled && lifecycle.compareAndSet(NOT_STARTED, FINISHED)) {
                bodyFinished.countDown();
            }
            return cancelled;
        }

        private void reject(RuntimeException failure) {
            if (lifecycle.compareAndSet(NOT_STARTED, FINISHED)) {
                super.setException(failure);
                bodyFinished.countDown();
            }
        }

        private void awaitBody() throws InterruptedException {
            bodyFinished.await();
        }
    }
}
