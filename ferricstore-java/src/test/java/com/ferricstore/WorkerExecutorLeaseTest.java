package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void aFailedBatchDoesNotReturnUntilItsRemainingTaskBodiesHaveDrained() throws Exception {
        WorkerExecutorLease lease = WorkerExecutorLease.create(2, false, null);
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch firstFailed = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (lease) {
            Future<?> batch =
                    caller.submit(
                            () ->
                                    assertThrows(
                                            IllegalStateException.class,
                                            () ->
                                                    WorkerExecutors.run(
                                                            List.of("fail", "slow"),
                                                            2,
                                                            lease,
                                                            item -> {
                                                                if ("fail".equals(item)) {
                                                                    try {
                                                                        assertTrue(
                                                                                slowStarted.await(
                                                                                        2,
                                                                                        TimeUnit
                                                                                                .SECONDS));
                                                                    } catch (
                                                                            InterruptedException
                                                                                    failure) {
                                                                        Thread.currentThread()
                                                                                .interrupt();
                                                                        throw new AssertionError(
                                                                                failure);
                                                                    }
                                                                    firstFailed.countDown();
                                                                    throw new IllegalStateException(
                                                                            "first failed");
                                                                }
                                                                slowStarted.countDown();
                                                                try {
                                                                    releaseSlow.await();
                                                                } catch (
                                                                        InterruptedException
                                                                                ignored) {
                                                                    // Deliberately model a handler
                                                                    // that ignores interruption.
                                                                    try {
                                                                        releaseSlow.await();
                                                                    } catch (
                                                                            InterruptedException
                                                                                    failure) {
                                                                        Thread.currentThread()
                                                                                .interrupt();
                                                                    }
                                                                }
                                                                return item;
                                                            })));

            assertTrue(firstFailed.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> batch.get(50, TimeUnit.MILLISECONDS));
            releaseSlow.countDown();
            batch.get(2, TimeUnit.SECONDS);
        } finally {
            releaseSlow.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void callerRunsPolicyNeverExecutesApplicationCodeOnTheSubmittingThread() throws Exception {
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        Future<?> blocker =
                executor.submit(
                        () -> {
                            occupied.countDown();
                            assertTrue(release.await(2, TimeUnit.SECONDS));
                            return null;
                        });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        WorkerExecutorLease lease = WorkerExecutorLease.create(1, false, executor);
        AtomicInteger applicationExecutions = new AtomicInteger();
        try (lease) {
            Future<String> rejected =
                    lease.<String>submitAll(
                                    List.of(
                                            () -> {
                                                applicationExecutions.incrementAndGet();
                                                return "must-not-run-inline";
                                            }))
                            .get(0);

            ExecutionException failure = assertThrows(ExecutionException.class, rejected::get);
            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            assertEquals(0, applicationExecutions.get());
        } finally {
            release.countDown();
            blocker.get(2, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }
}
