package com.ferricstore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkflowWorker {
    private final FerricStoreClient client;
    private final String type;
    private final String worker;
    private final List<String> states;
    private final Map<String, WorkflowHandler> handlers;
    private final Map<String, AsyncWorkflowHandler> asyncHandlers;
    private final int batchSize;
    private final int concurrency;
    private final boolean virtualThreads;
    private final ExecutorService executor;
    private final long leaseMs;
    private final Boolean reclaimExpired;
    private final String partitionKey;

    WorkflowWorker(
            FerricStoreClient client,
            String type,
            String worker,
            List<String> states,
            Map<String, WorkflowHandler> handlers,
            Map<String, AsyncWorkflowHandler> asyncHandlers) {
        this(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                WorkerExecutors.DEFAULT_BATCH_SIZE,
                1,
                false,
                null,
                30_000,
                null,
                null);
    }

    private WorkflowWorker(
            FerricStoreClient client,
            String type,
            String worker,
            List<String> states,
            Map<String, WorkflowHandler> handlers,
            Map<String, AsyncWorkflowHandler> asyncHandlers,
            int batchSize,
            int concurrency,
            boolean virtualThreads,
            ExecutorService executor,
            long leaseMs,
            Boolean reclaimExpired,
            String partitionKey) {
        this.client = client;
        this.type = type;
        this.worker = worker;
        this.states = states;
        this.handlers = handlers;
        this.asyncHandlers = asyncHandlers;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.virtualThreads = virtualThreads;
        this.executor = executor;
        this.leaseMs = leaseMs;
        this.reclaimExpired = reclaimExpired;
        this.partitionKey = partitionKey;
    }

    public WorkflowWorker batchSize(int batchSize) {
        WorkerExecutors.requirePositive("batchSize", batchSize);
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor,
                leaseMs,
                reclaimExpired,
                partitionKey);
    }

    public WorkflowWorker concurrency(int concurrency) {
        WorkerExecutors.requirePositive("concurrency", concurrency);
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor,
                leaseMs,
                reclaimExpired,
                partitionKey);
    }

    public WorkflowWorker virtualThreads() {
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                true,
                null,
                leaseMs,
                reclaimExpired,
                partitionKey);
    }

    public WorkflowWorker executor(ExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                false,
                executor,
                leaseMs,
                reclaimExpired,
                partitionKey);
    }

    /** Sets the lease duration used for claims made by this worker. */
    public WorkflowWorker leaseMs(long value) {
        FlowValidation.requirePositive(value, "leaseMs");
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor,
                value,
                reclaimExpired,
                partitionKey);
    }

    /** Controls whether this worker may take over expired claims. */
    public WorkflowWorker reclaimExpired(boolean value) {
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor,
                leaseMs,
                value,
                partitionKey);
    }

    /** Restricts this worker to one workflow partition. */
    public WorkflowWorker partitionKey(String value) {
        FlowValidation.requireText(value, "partitionKey");
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                asyncHandlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor,
                leaseMs,
                reclaimExpired,
                value);
    }

    public int runOnce() {
        try (WorkflowWorkerSession session = openSession()) {
            return session.runOnce();
        }
    }

    /** Runs one worker poll on a caller-owned executor without introducing a framework runtime. */
    public CompletableFuture<Integer> runOnceAsync(Executor executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        requireDispatchingExecutor(executor);
        return runStatesAsync(0, 0, executor);
    }

    private static void requireDispatchingExecutor(Executor executor) {
        Thread caller = Thread.currentThread();
        java.util.concurrent.atomic.AtomicBoolean inline =
                new java.util.concurrent.atomic.AtomicBoolean();
        executor.execute(
                () -> {
                    if (Thread.currentThread().equals(caller)) {
                        inline.set(true);
                    }
                });
        if (inline.get()) {
            throw new IllegalArgumentException(
                    "workflow executor must dispatch asynchronously; inline executors can deadlock transport callbacks");
        }
    }

    public WorkflowWorkerSession openSession() {
        return new WorkflowWorkerSession(this);
    }

    WorkerExecutorLease openExecutorLease() {
        return WorkerExecutorLease.create(concurrency, virtualThreads, executor);
    }

    int runOnce(WorkerExecutorLease executorLease) {
        int applied = 0;
        for (String state : states) {
            WorkflowHandler handler = handlers.get(state);
            AsyncWorkflowHandler asyncHandler = asyncHandlers.get(state);
            if (handler == null && asyncHandler == null) {
                throw new FerricStoreException("no workflow handler for state " + state);
            }
            int handledForState = 0;
            while (handledForState < batchSize) {
                int claimLimit = Math.min(concurrency, batchSize - handledForState);
                List<FlowRecord> jobs = client.claimDue(claimOptions(state, claimLimit));
                requireClaimLimit(jobs, claimLimit);
                if (jobs.isEmpty()) {
                    break;
                }
                handledForState +=
                        WorkerExecutors.run(
                                        jobs,
                                        concurrency,
                                        executorLease,
                                        job -> apply(job, state, handler, asyncHandler))
                                .size();
                if (jobs.size() < claimLimit) {
                    break;
                }
            }
            applied += handledForState;
        }
        return applied;
    }

    private ClaimDueOptions claimOptions(String state, int claimLimit) {
        ClaimDueOptions.Builder claim =
                ClaimDueOptions.builder(type, worker)
                        .state(state)
                        .partitionKey(partitionKey)
                        .payload(true)
                        .leaseMs(leaseMs)
                        .limit(claimLimit);
        if (reclaimExpired != null) {
            claim.reclaimExpired(reclaimExpired);
        }
        return claim.build();
    }

    private CompletableFuture<Integer> runStatesAsync(
            int stateIndex, int applied, Executor handlerExecutor) {
        if (stateIndex == states.size()) {
            return CompletableFuture.completedFuture(applied);
        }
        String state = states.get(stateIndex);
        WorkflowHandler handler = handlers.get(state);
        AsyncWorkflowHandler asyncHandler = asyncHandlers.get(state);
        if (handler == null && asyncHandler == null) {
            return CompletableFuture.failedFuture(
                    new FerricStoreException("no workflow handler for state " + state));
        }
        return runStateBatchesAsync(
                stateIndex, applied, 0, state, handler, asyncHandler, handlerExecutor);
    }

    private CompletableFuture<Integer> runStateBatchesAsync(
            int stateIndex,
            int applied,
            int handledForState,
            String state,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler,
            Executor handlerExecutor) {
        if (handledForState >= batchSize) {
            return runStatesAsync(stateIndex + 1, applied + handledForState, handlerExecutor);
        }
        int claimLimit = Math.min(concurrency, batchSize - handledForState);
        return AsyncFutures.compose(
                client.claimDueAsyncForWorker(claimOptions(state, claimLimit)),
                jobs ->
                        continueStateBatchAsync(
                                jobs,
                                claimLimit,
                                stateIndex,
                                applied,
                                handledForState,
                                state,
                                handler,
                                asyncHandler,
                                handlerExecutor));
    }

    private CompletableFuture<Integer> continueStateBatchAsync(
            List<FlowRecord> jobs,
            int claimLimit,
            int stateIndex,
            int applied,
            int handledForState,
            String state,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler,
            Executor handlerExecutor) {
        requireClaimLimit(jobs, claimLimit);
        if (jobs.isEmpty()) {
            return runStatesAsync(stateIndex + 1, applied + handledForState, handlerExecutor);
        }
        return AsyncFutures.compose(
                runJobsAsync(jobs, 0, state, handler, asyncHandler, handlerExecutor),
                handled -> {
                    int totalForState = handledForState + handled;
                    if (jobs.size() < claimLimit || totalForState >= batchSize) {
                        return runStatesAsync(
                                stateIndex + 1, applied + totalForState, handlerExecutor);
                    }
                    return runStateBatchesAsync(
                            stateIndex,
                            applied,
                            totalForState,
                            state,
                            handler,
                            asyncHandler,
                            handlerExecutor);
                });
    }

    private static void requireClaimLimit(List<FlowRecord> jobs, int claimLimit) {
        if (jobs.size() > claimLimit) {
            throw new FerricStoreException("FLOW.CLAIM_DUE returned more jobs than requested");
        }
    }

    private CompletableFuture<Integer> runJobsAsync(
            List<FlowRecord> jobs,
            int offset,
            String state,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler,
            Executor handlerExecutor) {
        if (offset >= jobs.size()) {
            return CompletableFuture.completedFuture(jobs.size());
        }
        int end = Math.min(jobs.size(), offset + concurrency);
        List<CompletableFuture<Void>> wave =
                jobs.subList(offset, end).stream()
                        .map(job -> applyAsync(job, state, handler, asyncHandler, handlerExecutor))
                        .toList();
        return AsyncFutures.compose(
                AsyncFutures.sequence(wave),
                ignored -> runJobsAsync(jobs, end, state, handler, asyncHandler, handlerExecutor));
    }

    private CompletableFuture<Void> applyAsync(
            FlowRecord job,
            String state,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler,
            Executor handlerExecutor) {
        WorkflowContext context =
                new WorkflowContext(
                        client,
                        job,
                        state,
                        DurableStepOptions.builder().leaseMs(leaseMs).build(),
                        handlerExecutor);
        CompletableFuture<Outcome> handled =
                invokeHandlerAsync(context, handler, asyncHandler, handlerExecutor);
        CompletableFuture<Void> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<?>> active = new AtomicReference<>(handled);
        handled.whenComplete(
                (outcome, handlerFailure) -> {
                    if (result.isDone()) {
                        return;
                    }
                    try {
                        if (handlerFailure != null) {
                            Throwable failure = AsyncFutures.unwrap(handlerFailure);
                            rethrowNonRetryable(failure, false);
                            ClaimedItem current = context.claimForFinalization();
                            CompletableFuture<Object> retry =
                                    client.retryAsyncForWorker(
                                            RetryOptions.builder(
                                                            current.id(),
                                                            current.leaseToken(),
                                                            current.fencingToken())
                                                    .partitionKey(current.partitionKey())
                                                    .error(errorPayload(failure))
                                                    .build());
                            active.set(retry);
                            if (result.isCancelled()) {
                                retry.cancel(false);
                                return;
                            }
                            completeVoid(retry, result);
                            return;
                        }
                        CompletableFuture<Object> mutation =
                                applyOutcomeAsync(context.claimForFinalization(), outcome);
                        active.set(mutation);
                        if (result.isCancelled()) {
                            mutation.cancel(false);
                            return;
                        }
                        completeVoid(mutation, result);
                    } catch (RuntimeException | Error terminal) {
                        result.completeExceptionally(terminal);
                    }
                });
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        active.get().cancel(false);
                    }
                });
        return result;
    }

    private CompletableFuture<Outcome> invokeHandlerAsync(
            WorkflowContext context,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler,
            Executor handlerExecutor) {
        CompletableFuture<Outcome> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<?>> active = new AtomicReference<>();
        Thread submittingThread = Thread.currentThread();
        java.util.concurrent.atomic.AtomicBoolean dispatchReturned =
                new java.util.concurrent.atomic.AtomicBoolean();
        try {
            handlerExecutor.execute(
                    () -> {
                        if (result.isDone()) {
                            return;
                        }
                        if (!dispatchReturned.get()
                                && Thread.currentThread().equals(submittingThread)) {
                            result.completeExceptionally(
                                    new FerricStoreException(
                                            "workflow executor ran a handler inline; no user code was executed"));
                            return;
                        }
                        try {
                            CompletionStage<Outcome> handled =
                                    handler != null
                                            ? CompletableFuture.completedFuture(
                                                    handler.handle(context))
                                            : asyncHandler.handle(context);
                            CompletableFuture<Outcome> userStage = stage(handled);
                            active.set(userStage);
                            if (result.isCancelled()) {
                                userStage.cancel(false);
                                return;
                            }
                            userStage.whenComplete(
                                    (outcome, failure) -> {
                                        if (failure == null) {
                                            result.complete(outcome);
                                        } else {
                                            result.completeExceptionally(
                                                    AsyncFutures.unwrap(failure));
                                        }
                                    });
                        } catch (Exception | Error failure) {
                            result.completeExceptionally(failure);
                        }
                    });
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
        } finally {
            dispatchReturned.set(true);
        }
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        context.cancelActiveMutation();
                        CompletableFuture<?> current = active.get();
                        if (current != null) {
                            current.cancel(false);
                        }
                    }
                });
        return result;
    }

    private CompletableFuture<Object> applyOutcomeAsync(ClaimedItem current, Outcome outcome) {
        if (outcome instanceof TransitionOutcome transition) {
            return client.transitionAsyncForWorker(
                    applyTransitionOutcome(
                                    TransitionOptions.builder(
                                            current.id(),
                                            current.state(),
                                            transition.toState(),
                                            current.leaseToken(),
                                            current.fencingToken()),
                                    transition)
                            .partitionKey(current.partitionKey())
                            .build());
        }
        if (outcome instanceof CompleteOutcome complete) {
            return client.completeAsyncForWorker(
                    CompleteOptions.builder(
                                    current.id(), current.leaseToken(), current.fencingToken())
                            .partitionKey(current.partitionKey())
                            .result(complete.result())
                            .payload(complete.payload())
                            .build());
        }
        if (outcome instanceof RetryOutcome retry) {
            return client.retryAsyncForWorker(
                    applyRetryOutcome(
                                    RetryOptions.builder(
                                            current.id(),
                                            current.leaseToken(),
                                            current.fencingToken()),
                                    retry)
                            .partitionKey(current.partitionKey())
                            .error(retry.error())
                            .payload(retry.payload())
                            .build());
        }
        if (outcome instanceof FailOutcome fail) {
            return client.failAsyncForWorker(
                    FailOptions.builder(current.id(), current.leaseToken(), current.fencingToken())
                            .partitionKey(current.partitionKey())
                            .error(fail.error())
                            .payload(fail.payload())
                            .build());
        }
        return CompletableFuture.completedFuture(null);
    }

    private static void completeVoid(
            CompletableFuture<?> source, CompletableFuture<Void> destination) {
        source.whenComplete(
                (ignored, failure) -> {
                    if (failure == null) {
                        destination.complete(null);
                    } else {
                        destination.completeExceptionally(AsyncFutures.unwrap(failure));
                    }
                });
    }

    private static <T> CompletableFuture<T> stage(CompletionStage<T> stage) {
        if (stage == null) {
            return CompletableFuture.failedFuture(
                    new FerricStoreException("asynchronous workflow handler returned null"));
        }
        return stage.toCompletableFuture();
    }

    private Void apply(
            FlowRecord job,
            String state,
            WorkflowHandler handler,
            AsyncWorkflowHandler asyncHandler) {
        WorkflowContext context =
                new WorkflowContext(
                        client, job, state, DurableStepOptions.builder().leaseMs(leaseMs).build());
        Outcome outcome;
        try {
            outcome =
                    handler != null
                            ? handler.handle(context)
                            : AsyncFutures.await(
                                    stage(asyncHandler.handle(context)),
                                    "asynchronous workflow handler was interrupted");
        } catch (Exception failure) {
            if (isCancellationOrInterruption(failure) || Thread.currentThread().isInterrupted()) {
                context.cancelActiveMutation();
            }
            rethrowNonRetryable(failure, true);
            ClaimedItem current = context.claimForFinalization();
            client.retry(
                    RetryOptions.builder(current.id(), current.leaseToken(), current.fencingToken())
                            .partitionKey(current.partitionKey())
                            .error(errorPayload(failure))
                            .build());
            return null;
        }

        ClaimedItem current = context.claimForFinalization();
        if (outcome instanceof TransitionOutcome transition) {
            client.transition(
                    applyTransitionOutcome(
                                    TransitionOptions.builder(
                                            current.id(),
                                            current.state(),
                                            transition.toState(),
                                            current.leaseToken(),
                                            current.fencingToken()),
                                    transition)
                            .partitionKey(current.partitionKey())
                            .build());
        } else if (outcome instanceof CompleteOutcome complete) {
            client.complete(
                    CompleteOptions.builder(
                                    current.id(), current.leaseToken(), current.fencingToken())
                            .partitionKey(current.partitionKey())
                            .result(complete.result())
                            .payload(complete.payload())
                            .build());
        } else if (outcome instanceof RetryOutcome retry) {
            client.retry(
                    applyRetryOutcome(
                                    RetryOptions.builder(
                                            current.id(),
                                            current.leaseToken(),
                                            current.fencingToken()),
                                    retry)
                            .partitionKey(current.partitionKey())
                            .error(retry.error())
                            .payload(retry.payload())
                            .build());
        } else if (outcome instanceof FailOutcome fail) {
            client.fail(
                    FailOptions.builder(current.id(), current.leaseToken(), current.fencingToken())
                            .partitionKey(current.partitionKey())
                            .error(fail.error())
                            .payload(fail.payload())
                            .build());
        }
        return null;
    }

    private static void rethrowNonRetryable(Throwable failure, boolean restoreInterrupt) {
        Throwable current = failure;
        java.util.Set<Throwable> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (current != null && seen.add(current)) {
            if (current instanceof InterruptedException) {
                if (restoreInterrupt) {
                    Thread.currentThread().interrupt();
                }
                throw new FerricStoreException(
                        "workflow handler was interrupted; no retry was written", failure);
            }
            if (current instanceof DurableMutationOutcomeUnknownException unknown) {
                throw unknown;
            }
            if (current instanceof CancellationException cancellation) {
                throw cancellation;
            }
            if (current instanceof StaleLeaseException staleLease) {
                throw staleLease;
            }
            if (current instanceof FlowWrongStateException wrongState) {
                throw wrongState;
            }
            if (current instanceof FlowNotFoundException notFound) {
                throw notFound;
            }
            if (current instanceof Error error) {
                throw error;
            }
            current = current.getCause();
        }
        if (restoreInterrupt && Thread.currentThread().isInterrupted()) {
            throw new FerricStoreException(
                    "workflow handler was interrupted; no retry was written", failure);
        }
    }

    private static boolean isCancellationOrInterruption(Throwable failure) {
        Throwable current = failure;
        java.util.Set<Throwable> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (current != null && seen.add(current)) {
            if (current instanceof InterruptedException
                    || current instanceof CancellationException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static TransitionOptions.Builder applyTransitionOutcome(
            TransitionOptions.Builder builder, TransitionOutcome transition) {
        builder.payload(transition.payload());
        if (transition.runAtMs() != null) {
            builder.runAtMs(transition.runAtMs());
        }
        if (transition.priority() != null) {
            builder.priority(transition.priority());
        }
        return builder;
    }

    private static RetryOptions.Builder applyRetryOutcome(
            RetryOptions.Builder builder, RetryOutcome retry) {
        if (retry.runAtMs() != null) {
            builder.runAtMs(retry.runAtMs());
        }
        return builder;
    }

    private static Map<String, String> errorPayload(Throwable e) {
        return Map.of("message", String.valueOf(e.getMessage()), "type", e.getClass().getName());
    }
}
