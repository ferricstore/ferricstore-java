package com.ferricstore;

import java.time.Duration;

/** A reusable, synchronous workflow-worker session with explicit resource ownership. */
public final class WorkflowWorkerSession implements AutoCloseable {
    private final WorkflowWorker worker;
    private final WorkerSessionRuntime runtime;

    WorkflowWorkerSession(WorkflowWorker worker) {
        this.worker = worker;
        this.runtime = new WorkerSessionRuntime(worker.openExecutorLease());
    }

    /** Claims and handles one batch for each configured state. */
    public int runOnce() {
        return runtime.run(() -> worker.runOnce(runtime.executorLease()));
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
