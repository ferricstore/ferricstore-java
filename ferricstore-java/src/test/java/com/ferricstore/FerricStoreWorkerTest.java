package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class FerricStoreWorkerTest {
    @Test
    void queueWorkerProcessesClaimedJobsConcurrently() {
        WorkerExecutor executor =
                new WorkerExecutor(
                        List.of(flowRecord("job-1", "queued"), flowRecord("job-2", "queued")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);
        Queue queue = new Queue(client, "email", "queued");
        CountDownLatch started = new CountDownLatch(2);

        QueueWorkerResult result =
                queue.worker("worker-1")
                        .batchSize(2)
                        .concurrency(2)
                        .runOnce(
                                job -> {
                                    started.countDown();
                                    assertTrue(started.await(2, TimeUnit.SECONDS));
                                    return "ok";
                                });

        assertEquals(new QueueWorkerResult(2, 2, 0, 0), result);
        assertEquals(1, executor.count("FLOW.CLAIM_DUE"));
        assertEquals(2, executor.count("FLOW.COMPLETE"));
        assertEquals(2, executor.claimLimit());
    }

    @Test
    void workflowWorkerProcessesClaimedJobsConcurrently() {
        WorkerExecutor executor =
                new WorkerExecutor(
                        List.of(flowRecord("flow-1", "created"), flowRecord("flow-2", "created")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);
        CountDownLatch started = new CountDownLatch(2);
        Workflow workflow =
                new Workflow(client, "order", "created")
                        .state(
                                "created",
                                ctx -> {
                                    started.countDown();
                                    assertTrue(started.await(2, TimeUnit.SECONDS));
                                    return Outcomes.transition("charged");
                                });

        int applied =
                workflow.worker("worker-1", List.of("created"))
                        .batchSize(2)
                        .concurrency(2)
                        .runOnce();

        assertEquals(2, applied);
        assertEquals(1, executor.count("FLOW.CLAIM_DUE"));
        assertEquals(2, executor.count("FLOW.TRANSITION"));
        assertEquals(2, executor.claimLimit());
    }

    @Test
    void workflowWorkerPreservesTheServerReclaimDefaultUnlessConfigured() {
        WorkerExecutor executor = new WorkerExecutor(List.of());
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(executor), "order", "created")
                        .state("created", ignored -> Outcomes.complete("unused"));

        workflow.worker("worker-1", List.of("created")).runOnce();

        assertFalse(executor.claimCommand().contains("RECLAIM_EXPIRED"));
    }

    @Test
    void queueFinalMutationFailureDoesNotTriggerAFallbackRetry() {
        NativeProtocolException responseLost =
                new NativeProtocolException("completion response was lost");
        WorkerExecutor executor =
                new WorkerExecutor(List.of(flowRecord("job-1", "queued")), responseLost);
        Queue queue = new Queue(FerricStoreClient.fromExecutor(executor), "email", "queued");

        assertThrows(
                NativeProtocolException.class,
                () -> queue.worker("worker-1").runOnce(job -> "sent"));

        assertEquals(1, executor.count("FLOW.COMPLETE"));
        assertEquals(0, executor.count("FLOW.RETRY"));
    }

    @Test
    void cancellingAsyncWorkerBeforeDequeuePreventsHandlerExecutionAndWrites() {
        WorkerExecutor commands = new WorkerExecutor(List.of(flowRecord("flow-1", "created")));
        AtomicInteger executions = new AtomicInteger();
        ManualExecutor handlers = new ManualExecutor();
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state(
                                "created",
                                ignored -> {
                                    executions.incrementAndGet();
                                    return Outcomes.complete("done");
                                });

        CompletableFuture<Integer> run =
                workflow.worker("worker-1", List.of("created")).runOnceAsync(handlers);
        assertTrue(run.cancel(false));
        handlers.runAll();

        assertEquals(0, executions.get());
        assertEquals(0, commands.count("FLOW.COMPLETE"));
        assertEquals(0, commands.count("FLOW.RETRY"));
    }

    @Test
    void cancellingAsyncWorkerCancelsTheUserCompletionStage() throws Exception {
        WorkerExecutor commands = new WorkerExecutor(List.of(flowRecord("flow-1", "created")));
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Outcome> userStage = new CompletableFuture<>();
        ExecutorService handlers = Executors.newSingleThreadExecutor();
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .stateAsync(
                                "created",
                                ignored -> {
                                    started.countDown();
                                    return userStage;
                                });

        try {
            CompletableFuture<Integer> run =
                    workflow.worker("worker-1", List.of("created")).runOnceAsync(handlers);
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertTrue(run.cancel(false));
            assertTrue(userStage.isCancelled());
            assertEquals(0, commands.count("FLOW.COMPLETE"));
            assertEquals(0, commands.count("FLOW.RETRY"));
        } finally {
            handlers.shutdownNow();
        }
    }

    @Test
    void asyncWorkerRejectsAnInlineExecutorBeforeClaimingWork() {
        WorkerExecutor commands = new WorkerExecutor(List.of(flowRecord("flow-1", "created")));
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state("created", ignored -> Outcomes.complete("done"));

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                workflow.worker("worker-1", List.of("created"))
                                        .runOnceAsync(Runnable::run));

        assertTrue(failure.getMessage().contains("dispatch asynchronously"));
        assertEquals(0, commands.count("FLOW.CLAIM_DUE"));
    }

    @Test
    void asyncWorkerNeverRunsAHandlerInlineWhenExecutorPolicyChanges() {
        WorkerExecutor commands = new WorkerExecutor(List.of(flowRecord("flow-1", "created")));
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger submissions = new AtomicInteger();
        Executor changesToInline =
                task -> {
                    if (submissions.getAndIncrement() == 0) {
                        new Thread(task, "executor-probe").start();
                    } else {
                        task.run();
                    }
                };
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state(
                                "created",
                                ignored -> {
                                    executions.incrementAndGet();
                                    return Outcomes.complete("done");
                                });

        assertEquals(
                1,
                workflow.worker("worker-1", List.of("created"))
                        .batchSize(1)
                        .runOnceAsync(changesToInline)
                        .join());

        assertEquals(0, executions.get());
        assertEquals(0, commands.count("FLOW.COMPLETE"));
        assertEquals(1, commands.count("FLOW.RETRY"));
    }

    @Test
    void workersNeverClaimMoreLeasesThanTheyCanStartImmediately() {
        WorkerExecutor commands = new WorkerExecutor(List.of());
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state("created", ignored -> Outcomes.complete("unused"));

        workflow.worker("worker-1", List.of("created")).batchSize(100).concurrency(1).runOnce();

        assertEquals(1, commands.claimLimit());
    }

    @Test
    void queueWorkersAlsoBoundClaimsByImmediateConcurrency() {
        WorkerExecutor commands = new WorkerExecutor(List.of());
        Queue queue = new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued");

        queue.worker("worker-1").batchSize(100).concurrency(1).runOnce(job -> "unused");

        assertEquals(1, commands.claimLimit());
    }

    @Test
    void runOnceFillsTheConfiguredBatchThroughImmediatelyStartableChunks() {
        SequencedClaimExecutor commands =
                new SequencedClaimExecutor(
                        List.of(flowRecord("flow-1", "created")),
                        List.of(flowRecord("flow-2", "created")));
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .state("created", ignored -> Outcomes.complete("done"));

        int handled =
                workflow.worker("worker-1", List.of("created"))
                        .batchSize(2)
                        .concurrency(1)
                        .runOnce();

        assertEquals(2, handled);
        assertEquals(List.of(1, 1), commands.claimLimits());
        assertEquals(2, commands.count("FLOW.COMPLETE"));
    }

    @Test
    void asyncRunOnceFillsTheConfiguredBatchThroughImmediatelyStartableChunks() {
        SequencedClaimExecutor commands =
                new SequencedClaimExecutor(
                        List.of(flowRecord("flow-1", "created")),
                        List.of(flowRecord("flow-2", "created")));
        Workflow workflow =
                new Workflow(FerricStoreClient.fromExecutor(commands), "order", "created")
                        .stateAsync(
                                "created",
                                ignored ->
                                        CompletableFuture.completedFuture(
                                                Outcomes.complete("done")));

        int handled =
                workflow.worker("worker-1", List.of("created"))
                        .batchSize(2)
                        .concurrency(1)
                        .runOnceAsync(CompletableFuture.delayedExecutor(0, TimeUnit.MILLISECONDS))
                        .join();

        assertEquals(2, handled);
        assertEquals(List.of(1, 1), commands.claimLimits());
        assertEquals(2, commands.count("FLOW.COMPLETE"));
    }

    @Test
    void queueRunOnceFillsTheConfiguredBatchThroughImmediatelyStartableChunks() {
        SequencedClaimExecutor commands =
                new SequencedClaimExecutor(
                        List.of(flowRecord("job-1", "queued")),
                        List.of(flowRecord("job-2", "queued")));
        Queue queue = new Queue(FerricStoreClient.fromExecutor(commands), "email", "queued");

        QueueWorkerResult handled =
                queue.worker("worker-1").batchSize(2).concurrency(1).runOnce(job -> "done");

        assertEquals(new QueueWorkerResult(2, 2, 0, 0), handled);
        assertEquals(List.of(1, 1), commands.claimLimits());
        assertEquals(2, commands.count("FLOW.COMPLETE"));
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

    private static final class WorkerExecutor implements CommandExecutor {
        private final Object claimResponse;
        private final RuntimeException mutationFailure;
        private final List<List<Object>> calls = Collections.synchronizedList(new ArrayList<>());

        private WorkerExecutor(Object claimResponse) {
            this(claimResponse, null);
        }

        private WorkerExecutor(Object claimResponse, RuntimeException mutationFailure) {
            this.claimResponse = claimResponse;
            this.mutationFailure = mutationFailure;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.get(0))) {
                return claimResponse;
            }
            if (mutationFailure != null) {
                throw mutationFailure;
            }
            return "OK";
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.get(0))).count();
            }
        }

        private int claimLimit() {
            synchronized (calls) {
                List<Object> claim = claimCommand();
                int limit = claim.indexOf("LIMIT");
                return ((Number) claim.get(limit + 1)).intValue();
            }
        }

        private List<Object> claimCommand() {
            synchronized (calls) {
                return calls.stream()
                        .filter(call -> "FLOW.CLAIM_DUE".equals(call.get(0)))
                        .findFirst()
                        .orElseThrow();
            }
        }
    }

    private static final class ManualExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.addLast(command);
        }

        private void runAll() {
            while (!tasks.isEmpty()) {
                tasks.removeFirst().run();
            }
        }
    }

    private static final class SequencedClaimExecutor implements CommandExecutor {
        private final Deque<Object> claims = new ArrayDeque<>();
        private final List<List<Object>> calls = Collections.synchronizedList(new ArrayList<>());

        private SequencedClaimExecutor(Object... claims) {
            Collections.addAll(this.claims, claims);
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.get(0))) {
                return claims.isEmpty() ? List.of() : claims.removeFirst();
            }
            return "OK";
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.get(0))).count();
            }
        }

        private List<Integer> claimLimits() {
            synchronized (calls) {
                return calls.stream()
                        .filter(call -> "FLOW.CLAIM_DUE".equals(call.get(0)))
                        .map(
                                call -> {
                                    int limit = call.indexOf("LIMIT");
                                    return ((Number) call.get(limit + 1)).intValue();
                                })
                        .toList();
            }
        }
    }
}
