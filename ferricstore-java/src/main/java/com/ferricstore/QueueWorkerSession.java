package com.ferricstore;

import java.time.Duration;

/** A reusable, synchronous queue-worker session with explicit resource ownership. */
public final class QueueWorkerSession implements AutoCloseable {
    private final QueueWorker worker;
    private final QueueHandler handler;
    private final WorkerSessionRuntime runtime;

    QueueWorkerSession(QueueWorker worker, QueueHandler handler) {
        this.worker = worker;
        this.handler = handler;
        this.runtime = new WorkerSessionRuntime(worker.openExecutorLease());
    }

    /** Claims and handles one batch. Only one call may be active per session. */
    public QueueWorkerResult runOnce() {
        return runtime.run(() -> worker.runOnce(handler, runtime.executorLease()));
    }

    /**
     * Stops accepting polls and waits up to {@code timeout} for the active poll to finish.
     *
     * @return {@code true} when the active poll drained, or {@code false} after cancellation was
     *     requested at the timeout
     */
    public boolean close(Duration timeout) {
        return runtime.close(timeout);
    }

    @Override
    public void close() {
        runtime.close();
    }
}
