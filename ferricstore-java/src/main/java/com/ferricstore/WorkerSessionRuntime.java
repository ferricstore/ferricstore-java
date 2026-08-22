package com.ferricstore;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

final class WorkerSessionRuntime implements AutoCloseable {
    static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final WorkerExecutorLease executorLease;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition idle = stateLock.newCondition();
    private State state = State.OPEN;
    private Thread activeCaller;
    private boolean drained = true;

    WorkerSessionRuntime(WorkerExecutorLease executorLease) {
        this.executorLease = executorLease;
    }

    WorkerExecutorLease executorLease() {
        return executorLease;
    }

    <T> T run(Supplier<T> poll) {
        Objects.requireNonNull(poll, "poll cannot be null");
        stateLock.lock();
        try {
            if (state == State.RUNNING) {
                throw new IllegalStateException("worker session poll is already running");
            }
            if (state != State.OPEN) {
                throw new IllegalStateException("worker session is closed");
            }
            state = State.RUNNING;
            activeCaller = Thread.currentThread();
        } finally {
            stateLock.unlock();
        }

        try {
            return poll.get();
        } finally {
            finishPoll();
        }
    }

    boolean close(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout cannot be null");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout cannot be negative");
        }
        long remainingNanos = durationToNanos(timeout);
        stateLock.lock();
        try {
            if (state == State.CLOSED) {
                return drained;
            }
            if (state == State.OPEN) {
                state = State.CLOSED;
                executorLease.close();
                return true;
            }
            if (Objects.equals(activeCaller, Thread.currentThread())) {
                throw new IllegalStateException(
                        "cannot close a worker session from its active poll thread");
            }
            state = State.CLOSING;
            while (state != State.CLOSED) {
                if (remainingNanos <= 0) {
                    drained = false;
                    state = State.CLOSED;
                    executorLease.cancelActive();
                    idle.signalAll();
                    return false;
                }
                try {
                    remainingNanos = idle.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    drained = false;
                    state = State.CLOSED;
                    executorLease.cancelActive();
                    idle.signalAll();
                    throw new FerricStoreException("interrupted while closing worker session", e);
                }
            }
            return drained;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT);
    }

    private void finishPoll() {
        stateLock.lock();
        try {
            activeCaller = null;
            if (state == State.RUNNING) {
                state = State.OPEN;
                return;
            }
            if (state == State.CLOSING) {
                state = State.CLOSED;
                executorLease.close();
                idle.signalAll();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private static long durationToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private enum State {
        OPEN,
        RUNNING,
        CLOSING,
        CLOSED
    }
}
