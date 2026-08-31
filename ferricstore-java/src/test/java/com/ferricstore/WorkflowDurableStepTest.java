package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class WorkflowDurableStepTest {
    @Test
    void contextRetainsTheRefreshedClaimAndWorkerFinalizesWithIt() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                        "OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicInteger executions = new AtomicInteger();
        Workflow workflow =
                new Workflow(client, "order", "charge")
                        .state(
                                "charge",
                                context -> {
                                    String result =
                                            context.step(
                                                    "charge-customer:v1",
                                                    () -> {
                                                        executions.incrementAndGet();
                                                        return "provider-42";
                                                    },
                                                    "schedule_warning",
                                                    String.class);
                                    assertEquals("provider-42", result);
                                    assertEquals("schedule_warning", context.state());
                                    assertEquals("lease-2", context.claim().leaseToken());
                                    assertEquals(8L, context.claim().fencingToken());
                                    return Outcomes.complete(result);
                                });

        int handled =
                workflow.worker("worker-a", List.of("charge"))
                        .batchSize(1)
                        .leaseMs(1_000)
                        .reclaimExpired(true)
                        .runOnce();

        assertEquals(1, handled);
        assertEquals(1, executions.get());
        assertEquals(
                List.of(
                        "FLOW.CLAIM_DUE",
                        "FLOW.EXTEND_LEASE",
                        "FLOW.STEP_CONTINUE",
                        "FLOW.COMPLETE"),
                commands.commandNames());
        List<Object> complete = commands.lastCall();
        assertEquals("lease-2", complete.get(2));
        assertOption(complete, "FENCING", 8L);
        List<Object> claim = commands.calls().get(0);
        assertOption(claim, "LEASE_MS", 1_000L);
        assertOption(claim, "RECLAIM_EXPIRED", "true");
        assertOption(commands.calls().get(1), "LEASE_MS", 1_000L);
        assertOption(commands.calls().get(2), "LEASE_MS", 1_000L);
    }

    @Test
    void uncertainStepCommitNeverTriggersAStaleRetry() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeProtocolException("connection failed after send"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        Workflow workflow =
                new Workflow(client, "order", "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.step(
                                            "charge-customer:v1",
                                            () -> "provider-42",
                                            "schedule_warning");
                                    return Outcomes.complete("unreachable");
                                });

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> workflow.worker("worker-a", List.of("charge")).runOnce());
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                commands.commandNames());
    }

    @Test
    void failedFinalMutationNeverTriggersASecondStaleMutation() {
        NativeProtocolException responseLost =
                new NativeProtocolException("connection failed after complete was sent");
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())), responseLost);
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        Workflow workflow =
                new Workflow(client, "order", "charge")
                        .state("charge", ignored -> Outcomes.complete("charged"));

        NativeProtocolException thrown =
                assertThrows(
                        NativeProtocolException.class,
                        () -> workflow.worker("worker-a", List.of("charge")).runOnce());

        assertEquals(responseLost, thrown);
        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.COMPLETE"), commands.commandNames());
    }

    @Test
    void failedAsyncFinalMutationNeverTriggersASecondStaleMutation() {
        NativeProtocolException responseLost =
                new NativeProtocolException("connection failed after complete was sent");
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())), responseLost);
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .stateAsync(
                                "charge",
                                ignored ->
                                        CompletableFuture.completedFuture(
                                                Outcomes.complete("charged")));

        CompletionException thrown =
                assertThrows(
                        CompletionException.class,
                        () ->
                                workflow.worker("worker-a", List.of("charge"))
                                        .runOnceAsync(
                                                CompletableFuture.delayedExecutor(
                                                        0, TimeUnit.MILLISECONDS))
                                        .join());

        assertEquals(responseLost, thrown.getCause());
        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.COMPLETE"), commands.commandNames());
    }

    @Test
    void uncertainDurableMutationIsPreservedByOwnedAndBorrowedWorkerExecutors() {
        assertConcurrentWorkerPreservesUnknown(null);
        ExecutorService borrowed = Executors.newFixedThreadPool(2);
        try {
            assertConcurrentWorkerPreservesUnknown(borrowed);
            assertFalse(borrowed.isShutdown());
        } finally {
            borrowed.shutdownNow();
        }
    }

    @Test
    void interruptedHandlerDoesNotPoisonTheCallerThreadOrWriteRetry() {
        WorkerExecutor commands =
                new WorkerExecutor(List.of(record("charge", "lease-1", 7L, Map.of())));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        Workflow workflow =
                new Workflow(client, "order", "charge")
                        .state(
                                "charge",
                                ignored -> {
                                    throw new InterruptedException("worker is stopping");
                                });

        try {
            FerricStoreException failure =
                    assertThrows(
                            FerricStoreException.class,
                            () -> workflow.worker("worker-a", List.of("charge")).runOnce());
            assertFalse(Thread.currentThread().isInterrupted());
            assertTrue(failure.getCause() instanceof InterruptedException);
            assertEquals(List.of("FLOW.CLAIM_DUE"), commands.commandNames());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedDurableClosureDoesNotPoisonTheCallerThreadOrWriteRetry() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        Workflow workflow =
                new Workflow(client, "order", "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.step(
                                            "charge-customer:v1",
                                            () -> {
                                                throw new InterruptedException(
                                                        "worker is stopping");
                                            },
                                            "schedule_warning");
                                    return Outcomes.complete("unreachable");
                                });

        try {
            FerricStoreException failure =
                    assertThrows(
                            FerricStoreException.class,
                            () -> workflow.worker("worker-a", List.of("charge")).runOnce());
            assertFalse(Thread.currentThread().isInterrupted());
            assertTrue(failure.getCause() instanceof FerricStoreException);
            assertTrue(failure.getCause().getCause() instanceof InterruptedException);
            assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE"), commands.commandNames());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void contextHydratesMetadataWrappedValueReferences() {
        WorkerExecutor commands = new WorkerExecutor(List.of(bytes("stored-value")));
        FlowRecord record =
                Resp.record(
                        record(
                                "charge",
                                "lease-1",
                                7L,
                                Map.of("input", Map.of("ref", "value-ref-1"))),
                        new StringCodec());
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        record,
                        "charge");

        assertEquals("stored-value", context.value("input"));
        assertEquals(List.of("FLOW.VALUE.MGET", "value-ref-1"), commands.lastCall());
    }

    @Test
    void contextAsyncTimingOverloadsRetainEachRefreshedClaim() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                        record("schedule_warning", "lease-2", 8L, Map.of()),
                        List.of("flow-1", "tenant-1", "lease-3", 9L));
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");
        DurableStepOptions options = DurableStepOptions.builder().leaseMs(1_000).nowMs(42).build();

        context.advanceAsync("schedule_warning", options).join();
        String result =
                context.stepAsync(
                                "notify:v1",
                                () -> CompletableFuture.completedFuture("sent"),
                                "done",
                                String.class,
                                options)
                        .join();

        assertEquals("sent", result);
        assertEquals("done", context.state());
        assertEquals("lease-3", context.claim().leaseToken());
        for (List<Object> command : commands.calls()) {
            assertOption(command, "LEASE_MS", 1_000L);
            assertOption(command, "NOW", 42L);
        }
    }

    @Test
    void asyncWorkflowHandlerComposesStepBeforeApplyingItsOutcome() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                        "OK");
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .stateAsync(
                                "charge",
                                context ->
                                        context.stepAsync(
                                                        "charge-customer:v1",
                                                        () ->
                                                                CompletableFuture.completedFuture(
                                                                        "provider-42"),
                                                        "schedule_warning",
                                                        String.class)
                                                .thenApply(Outcomes::complete));

        int handled =
                workflow.worker("worker-a", List.of("charge"))
                        .batchSize(1)
                        .runOnceAsync(CompletableFuture.delayedExecutor(0, TimeUnit.MILLISECONDS))
                        .join();

        assertEquals(1, handled);
        assertEquals(
                List.of(
                        "FLOW.CLAIM_DUE",
                        "FLOW.EXTEND_LEASE",
                        "FLOW.STEP_CONTINUE",
                        "FLOW.COMPLETE"),
                commands.commandNames());
        List<Object> complete = commands.lastCall();
        assertEquals("lease-2", complete.get(2));
        assertOption(complete, "FENCING", 8L);
    }

    @Test
    void contextRejectsAnOverlappingMutationBeforeItsClosureRuns() {
        PausingExecutor commands = new PausingExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");
        AtomicInteger executions = new AtomicInteger();

        CompletableFuture<String> first =
                context.stepAsync(
                        "first:v1",
                        () -> {
                            executions.incrementAndGet();
                            return CompletableFuture.completedFuture("one");
                        },
                        "next",
                        String.class);

        assertThrows(
                IllegalStateException.class,
                () ->
                        context.stepAsync(
                                "second:v1",
                                () -> {
                                    executions.incrementAndGet();
                                    return CompletableFuture.completedFuture("two");
                                },
                                "other",
                                String.class));
        assertEquals(0, executions.get());

        commands.preflight.complete(record("charge", "lease-1", 7L, Map.of()));
        assertEquals("one", first.join());
        assertEquals(1, executions.get());
        assertEquals(List.of("FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"), commands.commandNames());
    }

    @Test
    void cancelledContextMutationInvalidatesTheStaleClaimAndRequiresRecovery() {
        PausingExecutor commands = new PausingExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");
        AtomicInteger executions = new AtomicInteger();
        CompletableFuture<String> mutation =
                context.stepAsync(
                        "charge-customer:v1",
                        () -> {
                            executions.incrementAndGet();
                            return CompletableFuture.completedFuture("provider-42");
                        },
                        "schedule_warning",
                        String.class);

        assertTrue(mutation.cancel(false));
        assertTrue(mutation.isCancelled());
        assertThrows(CancellationException.class, mutation::join);
        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> context.advance("must-not-use-stale-claim"));
        assertEquals(0, executions.get());
        assertEquals(List.of("FLOW.EXTEND_LEASE"), commands.commandNames());
    }

    @Test
    void timeoutCannotDetachTheReturnedFutureFromTheActiveMutation() {
        PausingExecutor commands = new PausingExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                context.stepAsync(
                                                "charge-customer:v1",
                                                () -> CompletableFuture.completedFuture("charged"),
                                                "charged",
                                                String.class)
                                        .orTimeout(10, TimeUnit.MILLISECONDS)
                                        .join());

        assertTrue(failure.getCause() instanceof TimeoutException);
        assertTrue(commands.preflight.isCancelled());
        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> context.advance("must-not-use-stale-claim"));
    }

    @Test
    void externalExceptionalCompletionCannotDetachAnActiveMutation() {
        PausingExecutor commands = new PausingExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");
        CompletableFuture<String> mutation =
                context.stepAsync(
                        "charge-customer:v1",
                        () -> CompletableFuture.completedFuture("charged"),
                        "charged",
                        String.class);

        assertTrue(mutation.completeExceptionally(new TimeoutException("caller timeout")));

        assertTrue(commands.preflight.isCancelled());
        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> context.advance("must-not-use-stale-claim"));
    }

    @Test
    void timeoutFallbackCannotFabricateADurableResult() {
        PausingExecutor commands = new PausingExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                context.stepAsync(
                                                "charge-customer:v1",
                                                () -> CompletableFuture.completedFuture("charged"),
                                                "charged",
                                                String.class)
                                        .completeOnTimeout("fabricated", 10, TimeUnit.MILLISECONDS)
                                        .join());

        assertTrue(failure.getCause() instanceof DurableMutationOutcomeUnknownException);
        assertTrue(commands.preflight.isCancelled());
    }

    @Test
    void cancellingAWorkerCancelsTheAntecedentContextMutation() throws Exception {
        PausingWorkerExecutor commands = new PausingWorkerExecutor();
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .stateAsync(
                                "charge",
                                context ->
                                        context.stepAsync(
                                                        "charge-customer:v1",
                                                        () ->
                                                                CompletableFuture.completedFuture(
                                                                        "charged"),
                                                        "charged",
                                                        String.class)
                                                .thenApply(ignored -> Outcomes.complete("done")));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Integer> active =
                    workflow.worker("worker-a", List.of("charge")).runOnceAsync(executor);
            assertTrue(commands.preflightStarted.await(5, TimeUnit.SECONDS));

            assertTrue(active.cancel(false));

            assertTrue(commands.preflight.isCancelled());
            assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE"), commands.commandNames());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void workerRejectsAnOutcomeWhileAContextMutationIsStillActive() {
        PausingWorkerExecutor commands = new PausingWorkerExecutor();
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.stepAsync(
                                            "charge-customer:v1",
                                            () -> CompletableFuture.completedFuture("charged"),
                                            "charged",
                                            String.class);
                                    return Outcomes.complete("must-not-commit");
                                });

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> workflow.worker("worker-a", List.of("charge")).runOnce());
        assertTrue(commands.preflight.isCancelled());
        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE"), commands.commandNames());
    }

    @Test
    void asyncWorkerRejectsAnOutcomeWhileAContextMutationIsStillActive() {
        PausingWorkerExecutor commands = new PausingWorkerExecutor();
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .stateAsync(
                                "charge",
                                context -> {
                                    context.stepAsync(
                                            "charge-customer:v1",
                                            () -> CompletableFuture.completedFuture("charged"),
                                            "charged",
                                            String.class);
                                    return CompletableFuture.completedFuture(
                                            Outcomes.complete("must-not-commit"));
                                });

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                workflow.worker("worker-a", List.of("charge"))
                                        .runOnceAsync(
                                                CompletableFuture.delayedExecutor(
                                                        0, TimeUnit.MILLISECONDS))
                                        .join());
        assertTrue(failure.getCause() instanceof DurableMutationOutcomeUnknownException);
        assertTrue(commands.preflight.isCancelled());
        assertEquals(List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE"), commands.commandNames());
    }

    @Test
    void finalizationAtomicallySealsTheContextAgainstNewMutations() throws Exception {
        FinalizationRaceExecutor commands = new FinalizationRaceExecutor();
        AtomicReference<WorkflowContext> captured = new AtomicReference<>();
        AtomicReference<Object> mutationResult = new AtomicReference<>();
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    captured.set(context);
                                    return Outcomes.complete("done");
                                });
        Thread racer =
                new Thread(
                        () -> {
                            try {
                                assertTrue(commands.finalWriteStarted.await(2, TimeUnit.SECONDS));
                                mutationResult.set(captured.get().advance("raced"));
                            } catch (InterruptedException failure) {
                                Thread.currentThread().interrupt();
                                mutationResult.set(failure);
                            } catch (RuntimeException | Error failure) {
                                mutationResult.set(failure);
                            } finally {
                                commands.mutationAttempted.countDown();
                            }
                        },
                        "late-context-mutation");
        racer.start();

        assertEquals(1, workflow.worker("worker-a", List.of("charge")).batchSize(1).runOnce());
        racer.join(2_000);

        assertTrue(mutationResult.get() instanceof IllegalStateException);
        assertEquals(0, commands.count("FLOW.STEP_CONTINUE"));
        assertEquals(1, commands.count("FLOW.COMPLETE"));
    }

    @Test
    void rejectedContinuationExecutorPreservesSuccessfulAndUnknownMutationOutcomes() {
        Executor rejecting =
                ignored -> {
                    throw new RejectedExecutionException("shut down");
                };
        WorkflowContext successful =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(
                                new WorkerExecutor(
                                        record("charge", "lease-1", 7L, Map.of()),
                                        List.of("flow-1", "tenant-1", "lease-2", 8L)),
                                new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        rejecting);

        assertEquals(
                "provider-42",
                successful
                        .stepAsync(
                                "charge-customer:v1",
                                () -> CompletableFuture.completedFuture("provider-42"),
                                "schedule_warning",
                                String.class,
                                DurableStepOptions.defaults(),
                                ForkJoinPool.commonPool())
                        .join());

        WorkflowContext unknown =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(
                                new WorkerExecutor(
                                        record("charge", "lease-1", 7L, Map.of()),
                                        new NativeProtocolException("response lost")),
                                new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        rejecting);

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                unknown.stepAsync(
                                                "charge-customer:v1",
                                                () ->
                                                        CompletableFuture.completedFuture(
                                                                "provider-42"),
                                                "schedule_warning",
                                                String.class,
                                                DurableStepOptions.defaults(),
                                                ForkJoinPool.commonPool())
                                        .join());
        assertTrue(failure.getCause() instanceof DurableMutationOutcomeUnknownException);
        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> unknown.advance("must-not-use-stale-claim"));
    }

    @Test
    void rejectedContinuationExecutorNeverRunsUserCallbacksOnATransportReader() throws Exception {
        Executor rejecting =
                ignored -> {
                    throw new RejectedExecutionException("shut down");
                };
        NativeReaderCompletionExecutor commands = new NativeReaderCompletionExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        rejecting);
        AtomicReference<String> callbackThread = new AtomicReference<>();

        CompletableFuture<String> mutation =
                context.stepAsync(
                        "charge-customer:v1",
                        () -> CompletableFuture.completedFuture("provider-42"),
                        "schedule_warning",
                        String.class,
                        DurableStepOptions.defaults(),
                        ForkJoinPool.commonPool());
        CompletableFuture<String> dependent =
                mutation.thenApply(
                        value -> {
                            callbackThread.set(Thread.currentThread().getName());
                            return value;
                        });
        assertTrue(commands.responseStarted.await(2, TimeUnit.SECONDS));
        commands.allowResponse.countDown();

        assertEquals("provider-42", dependent.join());
        assertNotEquals("native-reader", callbackThread.get());
    }

    @Test
    void callerRunsContinuationExecutorNeverRunsUserCallbacksOnATransportReader() throws Exception {
        java.util.concurrent.CountDownLatch occupied = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ThreadPoolExecutor continuationExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        java.util.concurrent.Future<?> blocker =
                continuationExecutor.submit(
                        () -> {
                            occupied.countDown();
                            assertTrue(release.await(2, TimeUnit.SECONDS));
                            return null;
                        });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        NativeReaderCompletionExecutor commands = new NativeReaderCompletionExecutor();
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        continuationExecutor);
        AtomicReference<String> callbackThread = new AtomicReference<>();

        try {
            CompletableFuture<String> dependent =
                    context.stepAsync(
                                    "charge-customer:v1",
                                    () -> CompletableFuture.completedFuture("provider-42"),
                                    "schedule_warning",
                                    String.class,
                                    DurableStepOptions.defaults(),
                                    ForkJoinPool.commonPool())
                            .thenApply(
                                    value -> {
                                        callbackThread.set(Thread.currentThread().getName());
                                        return value;
                                    });
            assertTrue(commands.responseStarted.await(2, TimeUnit.SECONDS));
            commands.allowResponse.countDown();

            assertEquals("provider-42", dependent.join());
            assertNotEquals("native-reader", callbackThread.get());
        } finally {
            commands.allowResponse.countDown();
            release.countDown();
            blocker.get(2, TimeUnit.SECONDS);
            continuationExecutor.shutdownNow();
        }
    }

    @Test
    void completedMutationReleasesItsGuardBeforeDependentStagesRun() {
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(
                                new WorkerExecutor(
                                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                                        List.of("flow-1", "tenant-1", "lease-3", 9L)),
                                new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        Runnable::run);

        ClaimedItem finalClaim =
                context.advanceAsync("charged")
                        .thenCompose(ignored -> context.advanceAsync("done"))
                        .join();

        assertEquals("done", finalClaim.runState());
        assertEquals("lease-3", finalClaim.leaseToken());
    }

    @Test
    void caughtUnknownMutationPermanentlyInvalidatesTheContextClaim() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeProtocolException("response lost"));
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> context.step("charge-customer:v1", () -> "provider-42", "schedule_warning"));
        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> context.advance("must-not-use-stale-claim"));
        assertEquals(List.of("FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"), commands.commandNames());
    }

    @Test
    void failedCancellationAfterCompletionDoesNotPoisonTheRefreshedClaim() {
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(
                                new WorkerExecutor(
                                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                                        List.of("flow-1", "tenant-1", "lease-3", 9L)),
                                new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge",
                        DurableStepOptions.defaults(),
                        Runnable::run);

        CompletableFuture<ClaimedItem> first = context.advanceAsync("charged");
        assertEquals("charged", first.join().runState());
        assertFalse(first.cancel(false));
        assertEquals("done", context.advanceAsync("done").join().runState());
    }

    @Test
    void defaultAsyncStepUsesTheWorkerContinuationExecutorForTheClosure() {
        AtomicReference<String> closureThread = new AtomicReference<>();
        ExecutorService worker =
                Executors.newSingleThreadExecutor(
                        runnable -> new Thread(runnable, "workflow-owned"));
        try {
            WorkflowContext context =
                    new WorkflowContext(
                            FerricStoreClient.fromExecutor(
                                    new WorkerExecutor(
                                            record("charge", "lease-1", 7L, Map.of()),
                                            List.of("flow-1", "tenant-1", "lease-2", 8L)),
                                    new StringCodec()),
                            Resp.record(
                                    record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                            "charge",
                            DurableStepOptions.defaults(),
                            worker);

            assertEquals(
                    "provider-42",
                    context.stepAsync(
                                    "charge-customer:v1",
                                    () -> {
                                        closureThread.set(Thread.currentThread().getName());
                                        return CompletableFuture.completedFuture("provider-42");
                                    },
                                    "schedule_warning",
                                    String.class)
                            .join());
        } finally {
            worker.shutdownNow();
        }
        assertEquals("workflow-owned", closureThread.get());
    }

    @Test
    void unknownFutureNativeStatusNeverTriggersAStaleWorkerRetry() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeServerException(99, Map.of("message", "future status")));
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.step(
                                            "charge-customer:v1",
                                            () -> "provider-42",
                                            "schedule_warning");
                                    return Outcomes.complete("unreachable");
                                });

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> workflow.worker("worker-a", List.of("charge")).runOnce());
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                commands.commandNames());
    }

    @Test
    void httpRequestTimeoutAfterDispatchNeverTriggersAStaleWorkerRetry() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new HttpTransportException(
                                "request timed out after dispatch",
                                408,
                                "request_timeout",
                                true,
                                false,
                                null,
                                Map.of(),
                                null));
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.step(
                                            "charge-customer:v1",
                                            () -> "provider-42",
                                            "schedule_warning");
                                    return Outcomes.complete("unreachable");
                                });

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> workflow.worker("worker-a", List.of("charge")).runOnce());
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                commands.commandNames());
    }

    @Test
    void caughtUnknownStepStillPreventsSyncAndAsyncWorkerFinalization() {
        WorkerExecutor syncCommands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeProtocolException("response lost"));
        Workflow sync =
                new Workflow(
                                FerricStoreClient.fromExecutor(syncCommands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    try {
                                        context.step(
                                                "charge-customer:v1",
                                                () -> "provider-42",
                                                "schedule_warning");
                                    } catch (DurableMutationOutcomeUnknownException ignored) {
                                        // Simulate application code that attempts local recovery.
                                    }
                                    return Outcomes.complete("must-not-commit");
                                });

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () -> sync.worker("worker-a", List.of("charge")).runOnce());
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                syncCommands.commandNames());

        WorkerExecutor asyncCommands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeProtocolException("response lost"));
        Workflow async =
                new Workflow(
                                FerricStoreClient.fromExecutor(asyncCommands, new StringCodec()),
                                "order",
                                "charge")
                        .stateAsync(
                                "charge",
                                context ->
                                        context.stepAsync(
                                                        "charge-customer:v1",
                                                        () ->
                                                                CompletableFuture.completedFuture(
                                                                        "provider-42"),
                                                        "schedule_warning",
                                                        String.class)
                                                .handle(
                                                        (ignored, failure) ->
                                                                Outcomes.complete(
                                                                        "must-not-commit")));

        CompletionException asyncFailure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                async.worker("worker-a", List.of("charge"))
                                        .runOnceAsync(
                                                CompletableFuture.delayedExecutor(
                                                        0, TimeUnit.MILLISECONDS))
                                        .join());
        assertTrue(
                asyncFailure.getCause() instanceof DurableMutationOutcomeUnknownException,
                () -> String.valueOf(asyncFailure.getCause()));
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                asyncCommands.commandNames());
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void lostOwnershipDuringAStepNeverTriggersAFallbackRetry() {
        for (RuntimeException lost :
                List.of(
                        new StaleLeaseException("stale", null),
                        new FlowWrongStateException("wrong state", null),
                        new FlowNotFoundException("missing", null))) {
            WorkerExecutor commands =
                    new WorkerExecutor(
                            List.of(record("charge", "lease-1", 7L, Map.of())),
                            record("charge", "lease-1", 7L, Map.of()),
                            lost);
            Workflow workflow =
                    new Workflow(
                                    FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                    "order",
                                    "charge")
                            .state(
                                    "charge",
                                    context -> {
                                        context.step(
                                                "charge-customer:v1",
                                                () -> "provider-42",
                                                "schedule_warning");
                                        return Outcomes.complete("unreachable");
                                    });

            RuntimeException failure =
                    assertThrows(
                            RuntimeException.class,
                            () -> workflow.worker("worker-a", List.of("charge")).runOnce());
            assertEquals(lost, failure);
            assertEquals(
                    List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                    commands.commandNames());
        }
    }

    @Test
    void contextCustomDecoderHasSymmetricSyncAndAsyncTimingOverloads() {
        WorkerExecutor commands =
                new WorkerExecutor(
                        record("charge", "lease-1", 7L, Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                        record("schedule_warning", "lease-2", 8L, Map.of()),
                        List.of("flow-1", "tenant-1", "lease-3", 9L));
        WorkflowContext context =
                new WorkflowContext(
                        FerricStoreClient.fromExecutor(commands, new StringCodec()),
                        Resp.record(record("charge", "lease-1", 7L, Map.of()), new StringCodec()),
                        "charge");
        DurableStepOptions options = DurableStepOptions.builder().leaseMs(1_234).nowMs(42).build();
        DurableResultDecoder<String> uppercase =
                encoded ->
                        new String(encoded, StandardCharsets.UTF_8)
                                .toUpperCase(java.util.Locale.ROOT);

        String first =
                context.step(
                        "charge-customer:v1",
                        () -> "provider-42",
                        "schedule_warning",
                        uppercase,
                        options);
        String second =
                context.stepAsync(
                                "notify:v1",
                                () -> CompletableFuture.completedFuture("sent"),
                                "done",
                                uppercase,
                                options)
                        .join();

        assertEquals("PROVIDER-42", first);
        assertEquals("SENT", second);
        assertEquals("done", context.state());
        for (List<Object> command : commands.calls()) {
            assertOption(command, "LEASE_MS", 1_234L);
            assertOption(command, "NOW", 42L);
        }
    }

    private static Map<String, Object> record(
            String runState, String leaseToken, long fencingToken, Map<String, Object> references) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", "flow-1");
        response.put("type", "order");
        response.put("state", "running");
        response.put("run_state", runState);
        response.put("partition_key", "tenant-1");
        response.put("payload", bytes("payload"));
        response.put("lease_token", leaseToken);
        response.put("fencing_token", fencingToken);
        response.put("version", 10L);
        response.put("value_refs", references);
        return response;
    }

    private static void assertConcurrentWorkerPreservesUnknown(ExecutorService borrowed) {
        WorkerExecutor commands =
                new WorkerExecutor(
                        List.of(record("charge", "lease-1", 7L, Map.of())),
                        record("charge", "lease-1", 7L, Map.of()),
                        new NativeProtocolException("connection failed after send"));
        Workflow workflow =
                new Workflow(
                                FerricStoreClient.fromExecutor(commands, new StringCodec()),
                                "order",
                                "charge")
                        .state(
                                "charge",
                                context -> {
                                    context.step(
                                            "charge-customer:v1",
                                            () -> "provider-42",
                                            "schedule_warning");
                                    return Outcomes.complete("unreachable");
                                });
        WorkflowWorker worker = workflow.worker("worker-a", List.of("charge")).concurrency(2);
        if (borrowed != null) {
            worker = worker.executor(borrowed);
        }

        assertThrows(DurableMutationOutcomeUnknownException.class, worker::runOnce);
        assertEquals(
                List.of("FLOW.CLAIM_DUE", "FLOW.EXTEND_LEASE", "FLOW.STEP_CONTINUE"),
                commands.commandNames());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertOption(List<Object> command, String name, Object expected) {
        int index = command.indexOf(name);
        assertTrue(index >= 0, () -> "missing option " + name + " in " + command);
        assertEquals(expected, command.get(index + 1));
    }

    private static final class WorkerExecutor implements CommandExecutor {
        private final Deque<Object> responses = new ArrayDeque<>();
        private final List<List<Object>> calls = new ArrayList<>();

        private WorkerExecutor(Object... responses) {
            for (Object response : responses) {
                this.responses.addLast(response);
            }
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if (responses.isEmpty()) {
                throw new AssertionError("unexpected command: " + args);
            }
            Object response = responses.removeFirst();
            if (response instanceof RuntimeException failure) {
                throw failure;
            }
            return response;
        }

        private List<List<Object>> calls() {
            return calls;
        }

        private List<String> commandNames() {
            return calls.stream().map(call -> String.valueOf(call.get(0))).toList();
        }

        private List<Object> lastCall() {
            return calls.get(calls.size() - 1);
        }
    }

    private static final class PausingExecutor implements CommandExecutor {
        private final CompletableFuture<Object> preflight = new CompletableFuture<>();
        private final List<List<Object>> calls = new ArrayList<>();

        @Override
        public Object execute(List<Object> args) {
            return AsyncFutures.await(executeAsync(args), "test executor was interrupted");
        }

        @Override
        public CompletableFuture<Object> executeAsync(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.EXTEND_LEASE".equals(args.get(0))) {
                return preflight;
            }
            if ("FLOW.STEP_CONTINUE".equals(args.get(0))) {
                return CompletableFuture.completedFuture(
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
            }
            return CompletableFuture.failedFuture(
                    new AssertionError("unexpected command: " + args));
        }

        private List<String> commandNames() {
            return calls.stream().map(call -> String.valueOf(call.get(0))).toList();
        }
    }

    private static final class PausingWorkerExecutor implements CommandExecutor {
        private final CompletableFuture<Object> preflight = new CompletableFuture<>();
        private final java.util.concurrent.CountDownLatch preflightStarted =
                new java.util.concurrent.CountDownLatch(1);
        private final List<List<Object>> calls = new ArrayList<>();

        @Override
        public Object execute(List<Object> args) {
            return AsyncFutures.await(executeAsync(args), "test executor was interrupted");
        }

        @Override
        public CompletableFuture<Object> executeAsync(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.get(0))) {
                return CompletableFuture.completedFuture(
                        List.of(record("charge", "lease-1", 7L, Map.of())));
            }
            if ("FLOW.EXTEND_LEASE".equals(args.get(0))) {
                preflightStarted.countDown();
                return AsyncFutures.map(preflight, value -> value);
            }
            return CompletableFuture.completedFuture("OK");
        }

        private List<String> commandNames() {
            return calls.stream().map(call -> String.valueOf(call.get(0))).toList();
        }
    }

    private static final class NativeReaderCompletionExecutor implements CommandExecutor {
        private final java.util.concurrent.CountDownLatch responseStarted =
                new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch allowResponse =
                new java.util.concurrent.CountDownLatch(1);

        @Override
        public Object execute(List<Object> args) {
            return AsyncFutures.await(executeAsync(args), "test executor was interrupted");
        }

        @Override
        public CompletableFuture<Object> executeAsync(List<Object> args) {
            if ("FLOW.EXTEND_LEASE".equals(args.get(0))) {
                return CompletableFuture.completedFuture(record("charge", "lease-1", 7L, Map.of()));
            }
            if ("FLOW.STEP_CONTINUE".equals(args.get(0))) {
                CompletableFuture<Object> response = new CompletableFuture<>();
                new Thread(
                                () -> {
                                    responseStarted.countDown();
                                    try {
                                        allowResponse.await();
                                        response.complete(
                                                List.of("flow-1", "tenant-1", "lease-2", 8L));
                                    } catch (InterruptedException failure) {
                                        Thread.currentThread().interrupt();
                                        response.completeExceptionally(failure);
                                    }
                                },
                                "native-reader")
                        .start();
                return response;
            }
            return CompletableFuture.failedFuture(
                    new AssertionError("unexpected command: " + args));
        }
    }

    private static final class FinalizationRaceExecutor implements CommandExecutor {
        private final java.util.concurrent.CountDownLatch finalWriteStarted =
                new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch mutationAttempted =
                new java.util.concurrent.CountDownLatch(1);
        private final List<List<Object>> calls =
                java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.get(0))) {
                return List.of(record("charge", "lease-1", 7L, Map.of()));
            }
            if ("FLOW.COMPLETE".equals(args.get(0))) {
                finalWriteStarted.countDown();
                try {
                    if (!mutationAttempted.await(2, TimeUnit.SECONDS)) {
                        throw new AssertionError("late mutation did not run");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(failure);
                }
                return "OK";
            }
            if ("FLOW.STEP_CONTINUE".equals(args.get(0))) {
                return List.of("flow-1", "tenant-1", "lease-2", 8L);
            }
            throw new AssertionError("unexpected command: " + args);
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.get(0))).count();
            }
        }
    }
}
