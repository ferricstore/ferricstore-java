package com.ferricstore;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class AsyncPermitPool implements AutoCloseable {
    private static final ScheduledThreadPoolExecutor TIMEOUTS = timeoutExecutor();

    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final int maxWaiters;
    private int available;
    private boolean closed;

    AsyncPermitPool(int permits, int maxWaiters) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }
        if (maxWaiters <= 0) {
            throw new IllegalArgumentException("maxWaiters must be positive");
        }
        available = permits;
        this.maxWaiters = maxWaiters;
    }

    CompletableFuture<Permit> acquire(Long timeoutNanos) {
        synchronized (this) {
            if (closed) {
                return AsyncFutures.failed(new IllegalStateException("permit pool is closed"));
            }
            if (available > 0) {
                available--;
                return CompletableFuture.completedFuture(new Permit(this));
            }
            if (timeoutNanos != null && timeoutNanos <= 0) {
                return AsyncFutures.failed(new TimeoutException("capacity deadline exceeded"));
            }
            if (waiters.size() >= maxWaiters) {
                return AsyncFutures.failed(
                        new RejectedExecutionException("maximum pending requests exceeded"));
            }
            Waiter waiter = new Waiter();
            waiters.addLast(waiter);
            if (timeoutNanos != null) {
                waiter.timeout =
                        TIMEOUTS.schedule(
                                () -> timeout(waiter), timeoutNanos, TimeUnit.NANOSECONDS);
            }
            waiter.future.whenComplete(
                    (ignored, failure) -> {
                        if (waiter.future.isCancelled()) {
                            cancel(waiter);
                        }
                    });
            return waiter.future;
        }
    }

    private void cancel(Waiter waiter) {
        synchronized (this) {
            if (waiters.remove(waiter) && waiter.timeout != null) {
                waiter.timeout.cancel(false);
            }
        }
    }

    private void timeout(Waiter waiter) {
        boolean expired;
        synchronized (this) {
            expired = waiters.remove(waiter);
        }
        if (expired) {
            waiter.future.completeExceptionally(new TimeoutException("capacity deadline exceeded"));
        }
    }

    private void release() {
        Permit permit = new Permit(this);
        while (true) {
            Waiter waiter;
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (waiters.isEmpty()) {
                    available++;
                    return;
                }
                waiter = waiters.removeFirst();
                if (waiter.timeout != null) {
                    waiter.timeout.cancel(false);
                }
            }
            if (waiter.future.complete(permit)) {
                return;
            }
            // The waiter was cancelled after dequeue; offer the same permit to the next waiter.
        }
    }

    @Override
    public void close() {
        ArrayDeque<Waiter> closing;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closing = new ArrayDeque<>(waiters);
            waiters.clear();
        }
        IllegalStateException failure = new IllegalStateException("permit pool is closed");
        while (!closing.isEmpty()) {
            Waiter waiter = closing.removeFirst();
            if (waiter.timeout != null) {
                waiter.timeout.cancel(false);
            }
            waiter.future.completeExceptionally(failure);
        }
    }

    private static ScheduledThreadPoolExecutor timeoutExecutor() {
        ThreadFactory threads =
                runnable -> {
                    Thread thread = new Thread(runnable, "ferricstore-async-timeouts");
                    thread.setDaemon(true);
                    return thread;
                };
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threads);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    static final class Permit implements AutoCloseable {
        private final AsyncPermitPool owner;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(AsyncPermitPool owner) {
            this.owner = owner;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.release();
            }
        }
    }

    private static final class Waiter {
        private final CompletableFuture<Permit> future = new CompletableFuture<>();
        private ScheduledFuture<?> timeout;
    }
}
