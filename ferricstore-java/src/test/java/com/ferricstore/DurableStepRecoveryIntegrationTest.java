package com.ferricstore;

import static com.ferricstore.IntegrationTestEnvironment.assertExpectedHttpVersion;
import static com.ferricstore.IntegrationTestEnvironment.assumeIntegration;
import static com.ferricstore.IntegrationTestEnvironment.connectJson;
import static com.ferricstore.IntegrationTestEnvironment.suffix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

final class DurableStepRecoveryIntegrationTest {
    private static final long LEASE_MS = 500;
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(8);

    @Test
    void workerStopsBeforeCommitAndAnotherWorkerTakesOver() {
        assumeIntegration();
        for (boolean asyncWorker : List.of(false, true)) {
            runPreCommitTakeover(asyncWorker);
        }
    }

    @Test
    void externalOperationUsesOneStableProviderEffectAcrossTakeover() {
        assumeIntegration();
        for (boolean asyncWorker : List.of(false, true)) {
            runExternalEffectTakeover(asyncWorker);
        }
    }

    @Test
    void committedResponseLossReplaysTheStoredResultWithoutRunningTheClosure() {
        assumeIntegration();
        for (boolean asyncWorker : List.of(false, true)) {
            runCommittedResponseLoss(asyncWorker);
        }
    }

    @Test
    void realTransportDisconnectAfterCommitIsOutcomeUnknownAndRecoverable() {
        assumeIntegration();
        for (boolean asyncWorker : List.of(false, true)) {
            runTransportDisconnectAfterCommit(asyncWorker);
        }
    }

    @Test
    void waitingWorkflowReleasesItsWorkerAndResumesWithAFreshClaim() {
        assumeIntegration();
        for (boolean asyncWorker : List.of(false, true)) {
            runWaitingRecovery(asyncWorker);
        }
    }

    @Test
    void stoppedWorkerSessionWritesNothingAndAnotherWorkerTakesOver() throws Exception {
        assumeIntegration();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (FerricStoreClient client = connectJson()) {
            assertTransport(client);
            FlowIdentity flow = create(client, "session-stop", false);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch providerStopped = new CountDownLatch(1);
            CompletableFuture<String> providerStage = new CompletableFuture<>();
            providerStage.whenComplete((ignored, failure) -> providerStopped.countDown());
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            Workflow workflowA =
                    workflow(client, flow.type())
                            .stateAsync(
                                    "charge",
                                    context -> {
                                        claimA.set(context.claim());
                                        return context.stepAsync(
                                                        "charge-customer:v1",
                                                        () -> {
                                                            entered.countDown();
                                                            return providerStage;
                                                        },
                                                        "charged",
                                                        String.class)
                                                .thenApply(ignored -> Outcomes.complete("done"));
                                    });
            WorkflowWorkerSession session =
                    recoveryWorker(workflowA, "worker-a", "charge", flow.partition()).openSession();
            try (session) {
                Future<Integer> active = caller.submit(session::runOnce);
                assertTrue(entered.await(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                assertFalse(session.close(Duration.ZERO));
                assertThrows(
                        ExecutionException.class,
                        () -> active.get(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                assertTrue(
                        providerStopped.await(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                assertTrue(providerStage.isCancelled());
            }

            FlowRecord abandoned = client.get(flow.id(), flow.partition());
            assertEquals("running", abandoned.state());
            assertEquals("charge", abandoned.runState());
            assertEquals(claimA.get().leaseToken(), abandoned.leaseToken());

            Workflow workflowB =
                    state(
                            workflow(client, flow.type()),
                            "charge",
                            false,
                            context -> {
                                claimB.set(context.claim());
                                return step(context, false, () -> "provider-42", "schedule_warning")
                                        .thenApply(Outcomes::complete);
                            });
            awaitHandled(
                    recoveryWorker(workflowB, "worker-b", "charge", flow.partition()),
                    false,
                    caller);

            assertTakeover(claimA.get(), claimB.get(), "charge");
            assertEquals("completed", client.get(flow.id(), flow.partition()).state());
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void cancelledAsyncWorkerStageWritesNothingAndAnotherWorkerTakesOver() throws Exception {
        assumeIntegration();
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient client = connectJson()) {
            assertTransport(client);
            FlowIdentity flow = create(client, "async-cancel", true);
            CountDownLatch entered = new CountDownLatch(1);
            CompletableFuture<String> providerStage = new CompletableFuture<>();
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            Workflow workflowA =
                    workflow(client, flow.type())
                            .stateAsync(
                                    "charge",
                                    context -> {
                                        claimA.set(context.claim());
                                        return context.stepAsync(
                                                        "charge-customer:v1",
                                                        () -> {
                                                            entered.countDown();
                                                            return providerStage;
                                                        },
                                                        "charged",
                                                        String.class)
                                                .thenApply(ignored -> Outcomes.complete("done"));
                                    });

            CompletableFuture<Integer> active =
                    recoveryWorker(workflowA, "worker-a", "charge", flow.partition())
                            .runOnceAsync(async);
            assertTrue(entered.await(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(active.cancel(false));
            assertTrue(providerStage.isCancelled());

            FlowRecord abandoned = client.get(flow.id(), flow.partition());
            assertEquals("running", abandoned.state());
            assertEquals("charge", abandoned.runState());
            assertEquals(claimA.get().leaseToken(), abandoned.leaseToken());

            Workflow workflowB =
                    workflow(client, flow.type())
                            .state(
                                    "charge",
                                    context -> {
                                        claimB.set(context.claim());
                                        return Outcomes.complete("recovered");
                                    });
            awaitHandled(
                    recoveryWorker(workflowB, "worker-b", "charge", flow.partition()),
                    false,
                    async);

            assertTakeover(claimA.get(), claimB.get(), "charge");
            assertStaleClaimRejected(client, claimA.get());
            assertEquals("completed", client.get(flow.id(), flow.partition()).state());
        } finally {
            async.shutdownNow();
        }
    }

    private static void runPreCommitTakeover(boolean asyncWorker) {
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient control = connectJson();
                FerricStoreClient networkA = connectJson()) {
            assertTransport(control);
            FlowIdentity flow = create(control, "pre-commit", asyncWorker);
            AtomicInteger executions = new AtomicInteger();
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            FerricStoreClient workerAClient =
                    FerricStoreClient.fromExecutor(
                            new StepFailpointExecutor(
                                    networkA.commandExecutor(), FailurePoint.BEFORE_COMMIT),
                            new JsonCodec());
            Workflow workerAWorkflow =
                    state(
                            workflow(workerAClient, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimA.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "provider-42";
                                                },
                                                "schedule_warning")
                                        .thenApply(ignored -> Outcomes.complete("unreachable"));
                            });

            assertThrows(
                    RuntimeException.class,
                    () ->
                            run(
                                    recoveryWorker(
                                            workerAWorkflow,
                                            "worker-a",
                                            "charge",
                                            flow.partition()),
                                    asyncWorker,
                                    async));
            assertNotNull(claimA.get());

            Workflow workerBWorkflow =
                    state(
                            workflow(control, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimB.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "provider-42";
                                                },
                                                "schedule_warning")
                                        .thenApply(
                                                result -> {
                                                    assertEquals("provider-42", result);
                                                    assertStaleClaimRejected(control, claimA.get());
                                                    return Outcomes.complete(result);
                                                });
                            });
            awaitHandled(
                    recoveryWorker(workerBWorkflow, "worker-b", "charge", flow.partition()),
                    asyncWorker,
                    async);

            assertTakeover(claimA.get(), claimB.get(), "charge");
            assertEquals(2, executions.get());
            assertEquals("completed", control.get(flow.id(), flow.partition()).state());
        } finally {
            async.shutdownNow();
        }
    }

    private static void runTransportDisconnectAfterCommit(boolean asyncWorker) {
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient control = connectJson();
                ResponseDropProxy proxy =
                        ResponseDropProxy.open(IntegrationTestEnvironment.transportUrl());
                FerricStoreClient workerAClient =
                        IntegrationTestEnvironment.connectJsonAt(proxy.url())) {
            assertTransport(control);
            assertNotNull(workerAClient.ping());
            assertExpectedHttpVersion(workerAClient);
            FlowIdentity flow = create(control, "wire-response-loss", asyncWorker);
            AtomicInteger executions = new AtomicInteger();
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            Workflow workflowA =
                    state(
                            workflow(workerAClient, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimA.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    proxy.arm();
                                                    return "provider-42";
                                                },
                                                "schedule_warning")
                                        .thenApply(Outcomes::complete);
                            });

            assertThrows(
                    DurableMutationOutcomeUnknownException.class,
                    () ->
                            run(
                                    recoveryWorker(
                                            workflowA, "worker-a", "charge", flow.partition()),
                                    asyncWorker,
                                    async));
            proxy.awaitDropped(RECOVERY_TIMEOUT);

            FlowRecord committed = awaitRunState(control, flow, "schedule_warning");
            assertTrue(committed.fencingToken() > claimA.get().fencingToken());
            Workflow workflowB =
                    state(
                            workflow(control, flow.type()),
                            "schedule_warning",
                            asyncWorker,
                            context -> {
                                claimB.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "must-not-run";
                                                },
                                                "schedule_warning")
                                        .thenApply(Outcomes::complete);
                            });
            awaitHandled(
                    recoveryWorker(workflowB, "worker-b", "schedule_warning", flow.partition()),
                    asyncWorker,
                    async);

            assertTakeover(claimA.get(), claimB.get(), "schedule_warning");
            assertTrue(claimB.get().fencingToken() > committed.fencingToken());
            assertEquals(1, executions.get());
            assertEquals("completed", control.get(flow.id(), flow.partition()).state());
        } finally {
            async.shutdownNow();
        }
    }

    private static FlowRecord awaitRunState(
            FerricStoreClient client, FlowIdentity flow, String expectedRunState) {
        long deadline = System.nanoTime() + RECOVERY_TIMEOUT.toNanos();
        do {
            FlowRecord current = client.get(flow.id(), flow.partition());
            if (current != null && expectedRunState.equals(current.runState())) {
                return current;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new FerricStoreException("recovery read was interrupted", failure);
            }
        } while (System.nanoTime() < deadline);
        throw new AssertionError("committed workflow state was not visible before the deadline");
    }

    private static void runExternalEffectTakeover(boolean asyncWorker) {
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient control = connectJson();
                FerricStoreClient networkA = connectJson()) {
            assertTransport(control);
            FlowIdentity flow = create(control, "external-effect", asyncWorker);
            FakeProvider provider = new FakeProvider();
            String idempotencyKey = flow.id() + ":charge-customer:v1";
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            FerricStoreClient workerAClient =
                    FerricStoreClient.fromExecutor(
                            new StepFailpointExecutor(
                                    networkA.commandExecutor(), FailurePoint.BEFORE_COMMIT),
                            new JsonCodec());
            Workflow workerAWorkflow =
                    state(
                            workflow(workerAClient, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimA.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> provider.charge(idempotencyKey),
                                                "schedule_warning")
                                        .thenApply(ignored -> Outcomes.complete("unreachable"));
                            });
            assertThrows(
                    RuntimeException.class,
                    () ->
                            run(
                                    recoveryWorker(
                                            workerAWorkflow,
                                            "worker-a",
                                            "charge",
                                            flow.partition()),
                                    asyncWorker,
                                    async));

            Workflow workerBWorkflow =
                    state(
                            workflow(control, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimB.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> provider.charge(idempotencyKey),
                                                "schedule_warning")
                                        .thenApply(
                                                effect -> {
                                                    assertEquals("effect-1", effect);
                                                    assertStaleClaimRejected(control, claimA.get());
                                                    return Outcomes.complete(effect);
                                                });
                            });
            awaitHandled(
                    recoveryWorker(workerBWorkflow, "worker-b", "charge", flow.partition()),
                    asyncWorker,
                    async);

            assertTakeover(claimA.get(), claimB.get(), "charge");
            assertEquals(2, provider.calls());
            assertEquals(1, provider.createdEffects());
        } finally {
            async.shutdownNow();
        }
    }

    private static void runCommittedResponseLoss(boolean asyncWorker) {
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient control = connectJson();
                FerricStoreClient networkA = connectJson()) {
            assertTransport(control);
            FlowIdentity flow = create(control, "response-loss", asyncWorker);
            AtomicInteger executions = new AtomicInteger();
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            FerricStoreClient workerAClient =
                    FerricStoreClient.fromExecutor(
                            new StepFailpointExecutor(
                                    networkA.commandExecutor(), FailurePoint.AFTER_COMMIT),
                            new JsonCodec());
            Workflow workerAWorkflow =
                    state(
                            workflow(workerAClient, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimA.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "provider-42";
                                                },
                                                "schedule_warning")
                                        .thenApply(ignored -> Outcomes.complete("unreachable"));
                            });
            assertThrows(
                    DurableMutationOutcomeUnknownException.class,
                    () ->
                            run(
                                    recoveryWorker(
                                            workerAWorkflow,
                                            "worker-a",
                                            "charge",
                                            flow.partition()),
                                    asyncWorker,
                                    async));

            FlowRecord committed = control.get(flow.id(), flow.partition());
            assertEquals("running", committed.state());
            assertEquals("schedule_warning", committed.runState());
            assertTrue(committed.fencingToken() > claimA.get().fencingToken());

            Workflow workerBWorkflow =
                    state(
                            workflow(control, flow.type()),
                            "schedule_warning",
                            asyncWorker,
                            context -> {
                                claimB.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "must-not-run";
                                                },
                                                "schedule_warning")
                                        .thenApply(
                                                result -> {
                                                    assertEquals("provider-42", result);
                                                    assertStaleClaimRejected(control, claimA.get());
                                                    return Outcomes.complete(result);
                                                });
                            });
            awaitHandled(
                    recoveryWorker(
                            workerBWorkflow, "worker-b", "schedule_warning", flow.partition()),
                    asyncWorker,
                    async);

            assertTakeover(claimA.get(), claimB.get(), "schedule_warning");
            assertTrue(claimB.get().fencingToken() > committed.fencingToken());
            assertEquals(1, executions.get());
            FlowRecord completed = control.get(flow.id(), flow.partition());
            assertEquals("completed", completed.state());
            assertTrue(
                    completed.valueRefs().keySet().stream()
                            .anyMatch(name -> name.startsWith("__ferricstore_step__:sha256:")));
        } finally {
            async.shutdownNow();
        }
    }

    private static void runWaitingRecovery(boolean asyncWorker) {
        ExecutorService async = Executors.newSingleThreadExecutor();
        try (FerricStoreClient client = connectJson()) {
            assertTransport(client);
            FlowIdentity flow = create(client, "waiting", asyncWorker);
            AtomicInteger executions = new AtomicInteger();
            AtomicReference<ClaimedItem> claimA = new AtomicReference<>();
            AtomicReference<ClaimedItem> claimB = new AtomicReference<>();
            Workflow workflowA =
                    state(
                            workflow(client, flow.type()),
                            "charge",
                            asyncWorker,
                            context -> {
                                claimA.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "provider-42";
                                                },
                                                "waiting")
                                        .thenApply(
                                                ignored ->
                                                        new TransitionOutcome(
                                                                "waiting",
                                                                null,
                                                                System.currentTimeMillis() + 60_000,
                                                                null));
                            });
            assertEquals(
                    1,
                    run(
                            recoveryWorker(workflowA, "worker-a", "charge", flow.partition()),
                            asyncWorker,
                            async));
            FlowRecord waiting = client.get(flow.id(), flow.partition());
            assertEquals("waiting", waiting.state());
            assertEquals(null, waiting.leaseToken());
            assertEquals(1, executions.get());
            assertStaleClaimRejected(client, claimA.get());

            Workflow workflowB =
                    state(
                            workflow(client, flow.type()),
                            "waiting",
                            asyncWorker,
                            context -> {
                                claimB.set(context.claim());
                                return step(
                                                context,
                                                asyncWorker,
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return "must-not-run";
                                                },
                                                "waiting")
                                        .thenApply(
                                                result -> {
                                                    assertEquals("provider-42", result);
                                                    return Outcomes.complete(result);
                                                });
                            });
            WorkflowWorker workerB =
                    recoveryWorker(workflowB, "worker-b", "waiting", flow.partition());
            assertEquals(0, run(workerB, asyncWorker, async));

            long now = System.currentTimeMillis();
            assertNotNull(
                    client.signal(
                            flow.id(),
                            SignalOptions.builder("approved")
                                    .partitionKey(flow.partition())
                                    .idempotencyKey(flow.id() + ":approved")
                                    .ifState("waiting")
                                    .transitionTo("waiting")
                                    .runAtMs(now)
                                    .nowMs(now)
                                    .build()));
            assertEquals("waiting", client.get(flow.id(), flow.partition()).state());
            awaitHandled(workerB, asyncWorker, async);

            assertNotNull(claimB.get());
            assertNotEquals(claimA.get().leaseToken(), claimB.get().leaseToken());
            assertTrue(claimB.get().fencingToken() > waiting.fencingToken());
            assertEquals("waiting", claimB.get().runState());
            assertEquals(1, executions.get());
            assertEquals("completed", client.get(flow.id(), flow.partition()).state());
        } finally {
            async.shutdownNow();
        }
    }

    private static Workflow workflow(FerricStoreClient client, String type) {
        return new Workflow(client, type, "charge");
    }

    private static Workflow state(
            Workflow workflow,
            String state,
            boolean asyncWorker,
            Function<WorkflowContext, CompletionStage<Outcome>> handler) {
        if (asyncWorker) {
            return workflow.stateAsync(state, handler::apply);
        }
        return workflow.state(
                state,
                context ->
                        AsyncFutures.await(
                                handler.apply(context).toCompletableFuture(),
                                "recovery workflow handler was interrupted"));
    }

    private static CompletionStage<String> step(
            WorkflowContext context,
            boolean asyncWorker,
            Supplier<String> operation,
            String toState) {
        if (asyncWorker) {
            return context.stepAsync(
                    "charge-customer:v1",
                    () -> CompletableFuture.supplyAsync(operation),
                    toState,
                    String.class,
                    stepOptions());
        }
        return CompletableFuture.completedFuture(
                context.step(
                        "charge-customer:v1",
                        operation::get,
                        toState,
                        String.class,
                        stepOptions()));
    }

    private static WorkflowWorker recoveryWorker(
            Workflow workflow, String worker, String state, String partitionKey) {
        return workflow.worker(worker, List.of(state)).partitionKey(partitionKey).leaseMs(LEASE_MS);
    }

    private static FlowIdentity create(
            FerricStoreClient client, String scenario, boolean asyncWorker) {
        String token = suffix();
        String id = "java-sdk:durable-step:" + scenario + ":" + asyncWorker + ":" + token;
        String type = "java-sdk-durable-step-" + scenario + "-" + asyncWorker + "-" + token;
        String partition = "tenant-" + token;
        client.create(
                CreateOptions.builder(id, type)
                        .state("charge")
                        .partitionKey(partition)
                        .payload(Map.of("scenario", scenario))
                        .nowMs(System.currentTimeMillis())
                        .build());
        return new FlowIdentity(id, type, partition);
    }

    private static DurableStepOptions stepOptions() {
        return DurableStepOptions.builder().leaseMs(LEASE_MS).build();
    }

    private static int run(WorkflowWorker worker, boolean asyncWorker, ExecutorService executor) {
        return asyncWorker
                ? AsyncFutures.await(
                        worker.runOnceAsync(executor),
                        "asynchronous workflow worker was interrupted while waiting")
                : worker.runOnce();
    }

    private static void awaitHandled(
            WorkflowWorker worker, boolean asyncWorker, ExecutorService executor) {
        long deadline = System.nanoTime() + RECOVERY_TIMEOUT.toNanos();
        do {
            if (run(worker, asyncWorker, executor) == 1) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new FerricStoreException("recovery wait was interrupted", failure);
            }
        } while (System.nanoTime() < deadline);
        throw new AssertionError("worker B did not acquire the workflow before the deadline");
    }

    private static void assertTakeover(
            ClaimedItem claimA, ClaimedItem claimB, String expectedRunState) {
        assertNotNull(claimA);
        assertNotNull(claimB);
        assertEquals(expectedRunState, claimB.runState());
        assertEquals(claimA.partitionKey(), claimB.partitionKey());
        assertNotEquals(claimA.leaseToken(), claimB.leaseToken());
        assertTrue(claimB.fencingToken() > claimA.fencingToken());
    }

    private static void assertStaleClaimRejected(FerricStoreClient client, ClaimedItem stale) {
        FerricStoreException failure =
                assertThrows(
                        FerricStoreException.class,
                        () ->
                                client.complete(
                                        CompleteOptions.builder(
                                                        stale.id(),
                                                        stale.leaseToken(),
                                                        stale.fencingToken())
                                                .partitionKey(stale.partitionKey())
                                                .result("stale")
                                                .build()));
        String message = failure.getMessage().toLowerCase(java.util.Locale.ROOT);
        assertTrue(
                message.contains("lease") || message.contains("fenc") || message.contains("state"),
                () -> "unexpected stale-write failure: " + failure);
        assertNotNull(client.ping());
    }

    private static void assertTransport(FerricStoreClient client) {
        assertNotNull(client.ping());
        assertExpectedHttpVersion(client);
    }

    private enum FailurePoint {
        BEFORE_COMMIT,
        AFTER_COMMIT
    }

    private static final class StepFailpointExecutor implements CommandExecutor {
        private final CommandExecutor delegate;
        private final FailurePoint failurePoint;
        private final AtomicBoolean armed = new AtomicBoolean(true);

        private StepFailpointExecutor(CommandExecutor delegate, FailurePoint failurePoint) {
            this.delegate = delegate;
            this.failurePoint = failurePoint;
        }

        @Override
        public Object execute(List<Object> args) {
            boolean fail = target(args);
            if (fail && failurePoint == FailurePoint.BEFORE_COMMIT) {
                throw new WorkerStoppedException();
            }
            Object response = delegate.execute(args);
            if (fail && failurePoint == FailurePoint.AFTER_COMMIT) {
                throw new NativeProtocolException(
                        "test transport lost the committed STEP_CONTINUE response");
            }
            return response;
        }

        @Override
        public CompletableFuture<Object> executeAsync(List<Object> args) {
            boolean fail = target(args);
            if (fail && failurePoint == FailurePoint.BEFORE_COMMIT) {
                return CompletableFuture.failedFuture(new WorkerStoppedException());
            }
            CompletableFuture<Object> source = delegate.executeAsync(args);
            if (!fail || failurePoint != FailurePoint.AFTER_COMMIT) {
                return source;
            }
            return source.thenCompose(
                    ignored ->
                            CompletableFuture.failedFuture(
                                    new NativeProtocolException(
                                            "test transport lost the committed STEP_CONTINUE response")));
        }

        private boolean target(List<Object> args) {
            return !args.isEmpty()
                    && "FLOW.STEP_CONTINUE".equals(String.valueOf(args.get(0)))
                    && armed.compareAndSet(true, false);
        }
    }

    private static final class WorkerStoppedException extends CancellationException
            implements RequestDeliveryFailure {
        private static final long serialVersionUID = 1L;

        private WorkerStoppedException() {
            super("worker stopped before committing the durable step");
        }

        @Override
        public RequestDelivery delivery() {
            return RequestDelivery.NOT_SENT;
        }
    }

    private static final class FakeProvider {
        private final AtomicInteger calls = new AtomicInteger();
        private final java.util.concurrent.ConcurrentMap<String, String> effects =
                new java.util.concurrent.ConcurrentHashMap<>();

        private String charge(String idempotencyKey) {
            calls.incrementAndGet();
            return effects.computeIfAbsent(
                    idempotencyKey, ignored -> "effect-" + (effects.size() + 1));
        }

        private int calls() {
            return calls.get();
        }

        private int createdEffects() {
            return effects.size();
        }
    }

    private record FlowIdentity(String id, String type, String partition) {}
}
