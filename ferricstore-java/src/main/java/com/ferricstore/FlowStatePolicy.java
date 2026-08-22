package com.ferricstore;

/** Per-state scheduling and retry policy. */
public record FlowStatePolicy(FlowStateMode mode, RetryPolicy retry) {
    public static FlowStatePolicy fifo() {
        return fifo(null);
    }

    public static FlowStatePolicy fifo(RetryPolicy retry) {
        return new FlowStatePolicy(FlowStateMode.FIFO, retry);
    }

    public static FlowStatePolicy parallel() {
        return parallel(null);
    }

    public static FlowStatePolicy parallel(RetryPolicy retry) {
        return new FlowStatePolicy(FlowStateMode.PARALLEL, retry);
    }
}
