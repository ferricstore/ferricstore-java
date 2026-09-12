package com.ferricstore;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class WorkflowContext {
    private static final int MUTATION_IDLE = 0;
    private static final int MUTATION_ACTIVE = 1;
    private static final int MUTATION_FINALIZING = 2;

    private final FerricStoreClient client;
    private final FlowRecord job;
    private final DurableStepOptions defaultStepOptions;
    private final Executor continuationExecutor;
    private final AtomicInteger mutationState = new AtomicInteger(MUTATION_IDLE);
    private final AtomicReference<ContextMutationFuture<?>> activeMutation =
            new AtomicReference<>();
    private final AtomicReference<ClaimedItem> claim = new AtomicReference<>();
    private final AtomicReference<DurableMutationOutcomeUnknownException> invalidClaimFailure =
            new AtomicReference<>();

    WorkflowContext(FerricStoreClient client, FlowRecord job, String state) {
        this(
                client,
                job,
                state,
                DurableStepOptions.defaults(),
                java.util.concurrent.ForkJoinPool.commonPool());
    }

    WorkflowContext(
            FerricStoreClient client,
            FlowRecord job,
            String state,
            DurableStepOptions defaultStepOptions) {
        this(
                client,
                job,
                state,
                defaultStepOptions,
                java.util.concurrent.ForkJoinPool.commonPool());
    }

    WorkflowContext(
            FerricStoreClient client,
            FlowRecord job,
            String state,
            DurableStepOptions defaultStepOptions,
            Executor continuationExecutor) {
        this.client = client;
        this.job = job;
        this.defaultStepOptions =
                java.util.Objects.requireNonNull(defaultStepOptions, "defaultStepOptions");
        this.continuationExecutor =
                java.util.Objects.requireNonNull(continuationExecutor, "continuationExecutor");
        String runState = job.runState() == null ? state : job.runState();
        claim.set(
                new ClaimedItem(
                        job.id(),
                        job.leaseToken(),
                        job.fencingToken(),
                        job.partitionKey(),
                        job.type(),
                        "running",
                        runState,
                        job.payload(),
                        job.attributes()));
    }

    public FerricStoreClient client() {
        return client;
    }

    public FlowRecord job() {
        return job;
    }

    public String id() {
        return job.id();
    }

    public Object payload() {
        return job.payload();
    }

    public String state() {
        return claim().runState();
    }

    public String partitionKey() {
        return claim().partitionKey();
    }

    /** Returns the current claim, including any lease and fence refreshed by a durable step. */
    public ClaimedItem claim() {
        DurableMutationOutcomeUnknownException invalid = invalidClaimFailure.get();
        if (invalid != null) {
            throw invalid;
        }
        return claim.get();
    }

    ClaimedItem claimForFinalization() {
        DurableMutationOutcomeUnknownException invalid = invalidClaimFailure.get();
        if (invalid != null) {
            throw invalid;
        }
        if (mutationState.compareAndSet(MUTATION_IDLE, MUTATION_FINALIZING)) {
            return claim();
        }
        if (mutationState.compareAndSet(MUTATION_ACTIVE, MUTATION_FINALIZING)) {
            DurableMutationOutcomeUnknownException failure =
                    invalidateClaim(
                            new IllegalStateException(
                                    "workflow handler returned while a durable mutation was still active"));
            ContextMutationFuture<?> current = activeMutation.get();
            if (current != null) {
                current.abandon(failure);
            }
            throw failure;
        }
        throw new IllegalStateException("workflow context is already finalizing");
    }

    void cancelActiveMutation() {
        java.util.concurrent.CancellationException cancelled =
                new java.util.concurrent.CancellationException(
                        "asynchronous workflow execution was cancelled");
        invalidateClaim(cancelled);
        mutationState.set(MUTATION_FINALIZING);
        ContextMutationFuture<?> current = activeMutation.get();
        if (current != null) {
            current.cancel(false);
        }
    }

    /** Advances the workflow and retains the refreshed claim for subsequent handler operations. */
    public ClaimedItem advance(String toState) {
        return mutate(() -> retain(client.advance(claim(), toState, defaultStepOptions)));
    }

    /** Advances the workflow with explicit timing controls and retains the refreshed claim. */
    public ClaimedItem advance(String toState, DurableStepOptions options) {
        return mutate(() -> retain(client.advance(claim(), toState, options)));
    }

    /** Asynchronously advances the workflow and retains the refreshed claim. */
    public CompletableFuture<ClaimedItem> advanceAsync(String toState) {
        return mutateAsync(
                () ->
                        AsyncFutures.map(
                                client.advanceAsync(claim(), toState, defaultStepOptions),
                                this::retain));
    }

    /** Asynchronously advances with explicit timing controls and retains the refreshed claim. */
    public CompletableFuture<ClaimedItem> advanceAsync(String toState, DurableStepOptions options) {
        return mutateAsync(
                () ->
                        AsyncFutures.map(
                                client.advanceAsync(claim(), toState, options), this::retain));
    }

    /** Executes a durable closure and retains the refreshed claim. */
    public Object step(String name, Callable<?> run, String toState) {
        return mutate(
                () -> retainResult(client.step(claim(), name, run, toState, defaultStepOptions)));
    }

    /** Executes a durable closure with explicit timing controls and retains the refreshed claim. */
    public Object step(String name, Callable<?> run, String toState, DurableStepOptions options) {
        return mutate(() -> retainResult(client.step(claim(), name, run, toState, options)));
    }

    /** Executes a typed durable closure and retains the refreshed claim. */
    public <T> T step(String name, Callable<? extends T> run, String toState, Class<T> type) {
        return step(name, run, toState, type, defaultStepOptions);
    }

    /** Executes a typed durable closure using explicit timing controls. */
    public <T> T step(
            String name,
            Callable<? extends T> run,
            String toState,
            Class<T> type,
            DurableStepOptions options) {
        return mutate(() -> retainResult(client.step(claim(), name, run, toState, type, options)));
    }

    /** Executes a durable closure with a caller-supplied replay decoder. */
    public <T> T step(
            String name,
            Callable<? extends T> run,
            String toState,
            DurableResultDecoder<T> resultDecoder) {
        return step(name, run, toState, resultDecoder, defaultStepOptions);
    }

    /** Executes a custom-decoded durable closure using explicit timing controls. */
    public <T> T step(
            String name,
            Callable<? extends T> run,
            String toState,
            DurableResultDecoder<T> resultDecoder,
            DurableStepOptions options) {
        return mutate(
                () ->
                        retainResult(
                                client.step(claim(), name, run, toState, resultDecoder, options)));
    }

    /** Asynchronously executes a durable closure and retains the refreshed claim. */
    public CompletableFuture<Object> stepAsync(
            String name, Supplier<? extends CompletionStage<?>> run, String toState) {
        return mutateAsync(
                () ->
                        retainResultAsync(
                                client.stepAsync(
                                        claim(),
                                        name,
                                        run,
                                        toState,
                                        defaultStepOptions,
                                        continuationExecutor)));
    }

    /** Asynchronously executes a durable closure with explicit timing controls. */
    public CompletableFuture<Object> stepAsync(
            String name,
            Supplier<? extends CompletionStage<?>> run,
            String toState,
            DurableStepOptions options) {
        return mutateAsync(
                () ->
                        retainResultAsync(
                                client.stepAsync(
                                        claim(),
                                        name,
                                        run,
                                        toState,
                                        options,
                                        continuationExecutor)));
    }

    /** Invokes an asynchronous durable closure on a caller-owned executor. */
    public CompletableFuture<Object> stepAsync(
            String name,
            Supplier<? extends CompletionStage<?>> run,
            String toState,
            DurableStepOptions options,
            Executor closureExecutor) {
        return mutateAsync(
                () ->
                        retainResultAsync(
                                client.stepAsync(
                                        claim(), name, run, toState, options, closureExecutor)));
    }

    /** Asynchronously executes a typed durable closure and retains the refreshed claim. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            Class<T> type) {
        return stepAsync(name, run, toState, type, defaultStepOptions, continuationExecutor);
    }

    /** Asynchronously executes a typed durable closure with explicit timing controls. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            Class<T> type,
            DurableStepOptions options) {
        return stepAsync(name, run, toState, type, options, continuationExecutor);
    }

    /** Invokes a typed asynchronous durable closure on a caller-owned executor. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            Class<T> type,
            DurableStepOptions options,
            Executor closureExecutor) {
        return mutateAsync(
                () ->
                        retainResultAsync(
                                client.stepAsync(
                                        claim(),
                                        name,
                                        run,
                                        toState,
                                        type,
                                        options,
                                        closureExecutor)));
    }

    /** Asynchronously executes a durable closure with a caller-supplied replay decoder. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableResultDecoder<T> resultDecoder) {
        return stepAsync(name, run, toState, resultDecoder, defaultStepOptions);
    }

    /** Asynchronously executes a custom-decoded durable closure with timing controls. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableResultDecoder<T> resultDecoder,
            DurableStepOptions options) {
        return stepAsync(name, run, toState, resultDecoder, options, continuationExecutor);
    }

    /** Invokes a custom-decoded asynchronous closure on a caller-owned executor. */
    public <T> CompletableFuture<T> stepAsync(
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableResultDecoder<T> resultDecoder,
            DurableStepOptions options,
            Executor closureExecutor) {
        return mutateAsync(
                () ->
                        retainResultAsync(
                                client.stepAsync(
                                        claim(),
                                        name,
                                        run,
                                        toState,
                                        resultDecoder,
                                        options,
                                        closureExecutor)));
    }

    public Object value(String name) {
        Object direct = job.values().get(name);
        if (direct != null) {
            return direct;
        }
        if (!job.valueRefs().containsKey(name)) {
            return null;
        }
        String reference = DurableSteps.reference(job.valueRefs().get(name));
        return client.valueMGet(java.util.List.of(reference)).stream().findFirst().orElse(null);
    }

    private ClaimedItem retain(ClaimedItem refreshed) {
        claim.set(refreshed);
        return refreshed;
    }

    private <T> T retainResult(DurableStepResult<T> result) {
        retain(result.job());
        return result.result();
    }

    private <T> CompletableFuture<T> retainResultAsync(
            CompletableFuture<DurableStepResult<T>> source) {
        return AsyncFutures.map(source, this::retainResult);
    }

    private <T> T mutate(Callable<T> action) {
        beginMutation();
        try {
            return action.call();
        } catch (RuntimeException | Error failure) {
            rememberUnknown(failure);
            throw failure;
        } catch (Exception failure) {
            throw new FerricStoreException("durable workflow mutation failed", failure);
        } finally {
            mutationState.compareAndSet(MUTATION_ACTIVE, MUTATION_IDLE);
        }
    }

    private <T> CompletableFuture<T> mutateAsync(Supplier<CompletableFuture<T>> action) {
        beginMutation();
        CompletableFuture<T> source;
        try {
            source = java.util.Objects.requireNonNull(action.get(), "mutation future");
        } catch (RuntimeException | Error failure) {
            mutationState.compareAndSet(MUTATION_ACTIVE, MUTATION_IDLE);
            throw failure;
        }
        ContextMutationFuture<T> exposed = new ContextMutationFuture<>(source);
        activeMutation.set(exposed);
        if (invalidClaimFailure.get() != null) {
            exposed.cancel(false);
        }
        source.whenComplete(
                (value, failure) -> {
                    Runnable completion = () -> exposed.completeFromSource(value, failure);
                    AsyncFutures.dispatchCompletion(continuationExecutor, completion);
                });
        return exposed;
    }

    private void rememberUnknown(Throwable failure) {
        Throwable current = failure;
        java.util.Set<Throwable> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (current != null && seen.add(current)) {
            if (current instanceof DurableMutationOutcomeUnknownException unknown) {
                invalidClaimFailure.compareAndSet(null, unknown);
                return;
            }
            current = current.getCause();
        }
    }

    private void beginMutation() {
        DurableMutationOutcomeUnknownException invalid = invalidClaimFailure.get();
        if (invalid != null) {
            throw invalid;
        }
        if (!mutationState.compareAndSet(MUTATION_IDLE, MUTATION_ACTIVE)) {
            if (mutationState.get() == MUTATION_FINALIZING) {
                throw new IllegalStateException("workflow handler has already returned");
            }
            throw new IllegalStateException(
                    "another durable workflow mutation is already in progress");
        }
    }

    private DurableMutationOutcomeUnknownException invalidateClaim(Throwable cause) {
        DurableMutationOutcomeUnknownException failure = invalidClaimFailure.get();
        if (failure == null) {
            DurableMutationOutcomeUnknownException candidate =
                    new DurableMutationOutcomeUnknownException(
                            "cancelled durable workflow mutation", cause);
            if (invalidClaimFailure.compareAndSet(null, candidate)) {
                failure = candidate;
            } else {
                failure = invalidClaimFailure.get();
            }
        }
        return failure;
    }

    private final class ContextMutationFuture<T> extends CompletableFuture<T> {
        private final CompletableFuture<T> source;
        private final AtomicBoolean settled = new AtomicBoolean();

        private ContextMutationFuture(CompletableFuture<T> source) {
            this.source = source;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!settled.compareAndSet(false, true)) {
                return false;
            }
            invalidateClaim(
                    new java.util.concurrent.CancellationException(
                            "durable workflow mutation was cancelled"));
            releaseMutation();
            boolean accepted = super.cancel(mayInterruptIfRunning);
            source.cancel(mayInterruptIfRunning);
            return accepted;
        }

        @Override
        public boolean complete(T value) {
            return completeRecoveryRequired(
                    new IllegalStateException(
                            "a durable workflow mutation future cannot be completed externally"));
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return completeExternalFailure(
                    java.util.Objects.requireNonNull(failure, "failure"), true);
        }

        @Override
        public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
            java.util.Objects.requireNonNull(unit, "unit");
            if (!isDone()) {
                delayedExecutor(timeout, unit)
                        .execute(
                                () ->
                                        completeExternalFailure(
                                                new TimeoutException(
                                                        "durable workflow mutation timed out"),
                                                true));
            }
            return this;
        }

        @Override
        public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
            java.util.Objects.requireNonNull(unit, "unit");
            if (!isDone()) {
                delayedExecutor(timeout, unit)
                        .execute(
                                () ->
                                        completeRecoveryRequired(
                                                new TimeoutException(
                                                        "a timeout fallback cannot replace a durable result")));
            }
            return this;
        }

        @Override
        public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
            throw unsupportedExternalCompletion();
        }

        @Override
        public CompletableFuture<T> completeAsync(
                Supplier<? extends T> supplier, Executor executor) {
            throw unsupportedExternalCompletion();
        }

        @Override
        public void obtrudeValue(T value) {
            throw unsupportedExternalCompletion();
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw unsupportedExternalCompletion();
        }

        private void completeFromSource(T value, Throwable failure) {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            Throwable completionFailure = failure == null ? null : AsyncFutures.unwrap(failure);
            if (completionFailure instanceof java.util.concurrent.CancellationException) {
                completionFailure = invalidateClaim(completionFailure);
            } else if (completionFailure != null) {
                rememberUnknown(completionFailure);
            }
            releaseMutation();
            if (completionFailure == null) {
                super.complete(value);
            } else {
                super.completeExceptionally(completionFailure);
            }
        }

        private void abandon(DurableMutationOutcomeUnknownException failure) {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            releaseMutation();
            source.cancel(false);
            super.completeExceptionally(failure);
        }

        private boolean completeRecoveryRequired(Throwable cause) {
            return completeExternalFailure(cause, false);
        }

        private boolean completeExternalFailure(Throwable failure, boolean exposeOriginal) {
            if (!settled.compareAndSet(false, true)) {
                return false;
            }
            DurableMutationOutcomeUnknownException unknown = invalidateClaim(failure);
            releaseMutation();
            source.cancel(false);
            super.completeExceptionally(exposeOriginal ? failure : unknown);
            return true;
        }

        private void releaseMutation() {
            activeMutation.compareAndSet(this, null);
            mutationState.compareAndSet(MUTATION_ACTIVE, MUTATION_IDLE);
        }

        private UnsupportedOperationException unsupportedExternalCompletion() {
            return new UnsupportedOperationException(
                    "durable workflow mutation futures cannot be completed externally");
        }
    }
}
