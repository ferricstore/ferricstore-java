package com.ferricstore;

public record QueueWorkerResult(int claimed, int completed, int retried, int failed) {
}
