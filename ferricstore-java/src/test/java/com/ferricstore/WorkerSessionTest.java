package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class WorkerSessionTest {
    @Test
    void queueSessionReusesBorrowedExecutorWithoutOwningIt() {
        WorkerCommandExecutor commands =
                new WorkerCommandExecutor(
                        List.of(flowRecord("job-1", "queued"), flowRecord("job-2", "queued")));
        AtomicInteger createdThreads = new AtomicInteger();
        Set<String> handlerThreads = ConcurrentHashMap.newKeySet();
        ExecutorService workers =
                Executors.newFixedThreadPool(
                        2,
                        task ->
                                new Thread(
                                        task,
                                        "application-worker-" + createdThreads.incrementAndGet()));

        try {
            QueueWorker worker =
                    new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued")
                            .worker("worker-1")
                            .batchSize(2)
                            .concurrency(2)
                            .executor(workers);
            try (QueueWorkerSession session =
                    worker.openSession(
                            job -> {
                                handlerThreads.add(Thread.currentThread().getName());
                                return "ok";
                            })) {
                assertEquals(new QueueWorkerResult(2, 2, 0, 0), session.runOnce());
                assertEquals(new QueueWorkerResult(2, 2, 0, 0), session.runOnce());
            }

            assertFalse(workers.isShutdown());
            assertEquals(2, createdThreads.get());
            assertEquals(Set.of("application-worker-1", "application-worker-2"), handlerThreads);
            assertEquals(2, commands.count("FLOW.CLAIM_DUE"));
            assertEquals(4, commands.count("FLOW.COMPLETE"));
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void workflowSessionUsesTheSameCommandContractAcrossPolls() {
        WorkerCommandExecutor commands =
                new WorkerCommandExecutor(List.of(flowRecord("flow-1", "created")));
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state("created", context -> Outcomes.transition("charged"));

        try (WorkflowWorkerSession session =
                workflow.worker("worker-1", List.of("created")).openSession()) {
            assertEquals(1, session.runOnce());
            assertEquals(1, session.runOnce());
        }

        assertEquals(2, commands.count("FLOW.CLAIM_DUE"));
        assertEquals(2, commands.count("FLOW.TRANSITION"));
    }

    @Test
    void sessionRejectsOverlappingPolls() throws Exception {
        WorkerCommandExecutor commands =
                new WorkerCommandExecutor(List.of(flowRecord("job-1", "queued")));
        CountDownLatch enteredHandler = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        QueueWorkerSession session =
                new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued")
                        .worker("worker-1")
                        .openSession(
                                job -> {
                                    enteredHandler.countDown();
                                    assertTrue(releaseHandler.await(2, TimeUnit.SECONDS));
                                    return "ok";
                                });
        ExecutorService caller = Executors.newSingleThreadExecutor();

        try (session) {
            try {
                Future<QueueWorkerResult> active = caller.submit(session::runOnce);
                assertTrue(enteredHandler.await(2, TimeUnit.SECONDS));
                IllegalStateException error =
                        assertThrows(IllegalStateException.class, session::runOnce);
                assertTrue(error.getMessage().contains("already running"));
                releaseHandler.countDown();
                assertEquals(new QueueWorkerResult(1, 1, 0, 0), active.get(2, TimeUnit.SECONDS));
            } finally {
                releaseHandler.countDown();
                caller.shutdownNow();
            }
        }
    }

    @Test
    void closeDrainsAnActivePollAndThenRejectsFurtherUse() throws Exception {
        WorkerCommandExecutor commands =
                new WorkerCommandExecutor(List.of(flowRecord("job-1", "queued")));
        CountDownLatch enteredHandler = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        QueueWorkerSession session =
                new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued")
                        .worker("worker-1")
                        .openSession(
                                job -> {
                                    enteredHandler.countDown();
                                    assertTrue(releaseHandler.await(2, TimeUnit.SECONDS));
                                    return "ok";
                                });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        ExecutorService releaser = Executors.newSingleThreadExecutor();

        try (session) {
            try {
                Future<QueueWorkerResult> active = caller.submit(session::runOnce);
                assertTrue(enteredHandler.await(2, TimeUnit.SECONDS));
                Future<?> released =
                        releaser.submit(
                                () -> {
                                    try {
                                        Thread.sleep(25);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                    releaseHandler.countDown();
                                });

                assertTrue(session.close(Duration.ofSeconds(1)));
                released.get(2, TimeUnit.SECONDS);
                assertEquals(new QueueWorkerResult(1, 1, 0, 0), active.get(2, TimeUnit.SECONDS));
                IllegalStateException error =
                        assertThrows(IllegalStateException.class, session::runOnce);
                assertTrue(error.getMessage().contains("closed"));
            } finally {
                releaseHandler.countDown();
                caller.shutdownNow();
                releaser.shutdownNow();
            }
        }
    }

    @Test
    void closeTimeoutCancelsTasksButPreservesABorrowedExecutor() throws Exception {
        WorkerCommandExecutor commands =
                new WorkerCommandExecutor(List.of(flowRecord("job-1", "queued")));
        CountDownLatch enteredHandler = new CountDownLatch(1);
        CountDownLatch interruptedHandler = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        QueueWorkerSession session =
                new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued")
                        .worker("worker-1")
                        .concurrency(2)
                        .executor(workers)
                        .openSession(
                                job -> {
                                    enteredHandler.countDown();
                                    try {
                                        new CountDownLatch(1).await();
                                    } catch (InterruptedException e) {
                                        interruptedHandler.countDown();
                                        throw e;
                                    }
                                    return "unreachable";
                                });

        try (session) {
            try {
                Future<QueueWorkerResult> active = caller.submit(session::runOnce);
                assertTrue(enteredHandler.await(2, TimeUnit.SECONDS));

                assertFalse(session.close(Duration.ZERO));
                assertTrue(interruptedHandler.await(2, TimeUnit.SECONDS));
                ExecutionException error =
                        assertThrows(
                                ExecutionException.class, () -> active.get(2, TimeUnit.SECONDS));
                FerricStoreException workerError =
                        assertInstanceOf(FerricStoreException.class, error.getCause());
                assertTrue(workerError.getMessage().contains("cancelled"));
                assertFalse(workers.isShutdown());
            } finally {
                caller.shutdownNow();
                workers.shutdownNow();
            }
        }
    }

    private static Object flowRecord(String id, String state) {
        return Resp.testMap(
                "id",
                id,
                "type",
                "test",
                "state",
                state,
                "partition_key",
                "p1",
                "lease_token",
                "lease-" + id,
                "fencing_token",
                1L,
                "version",
                1L);
    }

    private static final class WorkerCommandExecutor implements CommandExecutor {
        private final Object claimResponse;
        private final List<List<Object>> calls = Collections.synchronizedList(new ArrayList<>());

        private WorkerCommandExecutor(Object claimResponse) {
            this.claimResponse = claimResponse;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.get(0))) {
                return claimResponse;
            }
            return "OK";
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.get(0))).count();
            }
        }
    }
}
