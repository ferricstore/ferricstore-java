package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

final class WorkerExecutorLeaseTest {
    @Test
    void closesAnSdkOwnedExecutor() {
        WorkerExecutorLease lease = WorkerExecutorLease.create(2, false, null);
        ExecutorService executor = lease.executor();

        lease.close();

        assertTrue(executor.isShutdown());
    }

    @Test
    void neverClosesABorrowedExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            WorkerExecutorLease lease = WorkerExecutorLease.create(2, false, executor);

            lease.close();

            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }
}
