package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

final class DurableSteps {
    private static final String VALUE_PREFIX = "__ferricstore_step__:sha256:";
    private final CommandExecutor executor;
    private final Codec codec;

    DurableSteps(CommandExecutor executor, Codec codec) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    ClaimedItem advance(ClaimedItem job, String toState) {
        return advance(job, toState, DurableStepOptions.defaults());
    }

    ClaimedItem advance(ClaimedItem job, String toState, DurableStepOptions options) {
        return AsyncFutures.await(
                advanceAsync(job, toState, options),
                interrupted ->
                        unknown(
                                "FLOW.STEP_CONTINUE",
                                new FerricStoreException(
                                        "interrupted while awaiting durable workflow mutation",
                                        interrupted)));
    }

    CompletableFuture<ClaimedItem> advanceAsync(ClaimedItem job, String toState) {
        return advanceAsync(job, toState, DurableStepOptions.defaults());
    }

    CompletableFuture<ClaimedItem> advanceAsync(
            ClaimedItem job, String toState, DurableStepOptions options) {
        validateClaim(job, toState);
        Objects.requireNonNull(options, "options");
        return continueAsync(job, toState, null, options);
    }

    static ClaimedItem compact(ClaimedFlow job) {
        Objects.requireNonNull(job, "job");
        if (job instanceof ClaimedItem compact) {
            return compact;
        }
        return new ClaimedItem(
                job.id(),
                job.leaseToken(),
                job.fencingToken(),
                job.partitionKey(),
                job.type(),
                job.state(),
                job.runState(),
                job.payload(),
                job.attributes());
    }

    DurableStepResult<Object> step(ClaimedItem job, String name, Callable<?> run, String toState) {
        return step(job, name, run, toState, DurableStepOptions.defaults());
    }

    DurableStepResult<Object> step(
            ClaimedItem job,
            String name,
            Callable<?> run,
            String toState,
            DurableStepOptions options) {
        return step(job, name, run, toState, options, codec::decode);
    }

    <T> DurableStepResult<T> step(
            ClaimedItem job,
            String name,
            Callable<? extends T> run,
            String toState,
            DurableStepOptions options,
            Class<T> resultType) {
        Objects.requireNonNull(resultType, "resultType");
        return step(
                job, name, run, toState, options, decoder(resultType), resultType == Void.class);
    }

    <T> DurableStepResult<T> step(
            ClaimedItem job,
            String name,
            Callable<? extends T> run,
            String toState,
            DurableStepOptions options,
            DurableResultDecoder<T> resultDecoder) {
        return step(job, name, run, toState, options, resultDecoder, false);
    }

    private <T> DurableStepResult<T> step(
            ClaimedItem job,
            String name,
            Callable<? extends T> run,
            String toState,
            DurableStepOptions options,
            DurableResultDecoder<T> resultDecoder,
            boolean allowNullResult) {
        validateStep(job, name, run, toState);
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(resultDecoder, "resultDecoder");
        FlowRecord current = preflight(job, options);
        String valueName = valueName(name);
        String reference = committedReference(current, valueName);
        if (reference != null) {
            requireTargetState(current, toState);
            return new DurableStepResult<>(
                    claimFromRecord(current, job), storedResult(reference, resultDecoder), true);
        }

        Object value;
        try {
            value = run.call();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new FerricStoreException("durable step closure was interrupted", failure);
        } catch (Exception failure) {
            throw new FerricStoreException("durable step closure failed", failure);
        }
        EncodedResult<T> encoded = encode(value, resultDecoder, allowNullResult);
        ClaimedItem refreshed =
                AsyncFutures.await(
                        continueAsync(
                                job, toState, new CommitValue(valueName, encoded.bytes()), options),
                        interrupted ->
                                unknown(
                                        "FLOW.STEP_CONTINUE",
                                        new FerricStoreException(
                                                "interrupted while awaiting durable workflow mutation",
                                                interrupted)));
        return new DurableStepResult<>(refreshed, encoded.normalized(), false);
    }

    CompletableFuture<DurableStepResult<Object>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<?>> run,
            String toState) {
        return stepAsync(
                job, name, run, toState, DurableStepOptions.defaults(), ForkJoinPool.commonPool());
    }

    CompletableFuture<DurableStepResult<Object>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<?>> run,
            String toState,
            DurableStepOptions options) {
        return stepAsync(job, name, run, toState, options, ForkJoinPool.commonPool());
    }

    CompletableFuture<DurableStepResult<Object>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<?>> run,
            String toState,
            DurableStepOptions options,
            Executor closureExecutor) {
        return stepAsync(job, name, run, toState, options, closureExecutor, codec::decode);
    }

    <T> CompletableFuture<DurableStepResult<T>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableStepOptions options,
            Executor closureExecutor,
            Class<T> resultType) {
        Objects.requireNonNull(resultType, "resultType");
        return stepAsync(
                job,
                name,
                run,
                toState,
                options,
                closureExecutor,
                decoder(resultType),
                resultType == Void.class);
    }

    <T> CompletableFuture<DurableStepResult<T>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableStepOptions options,
            Executor closureExecutor,
            DurableResultDecoder<T> resultDecoder) {
        return stepAsync(job, name, run, toState, options, closureExecutor, resultDecoder, false);
    }

    private <T> CompletableFuture<DurableStepResult<T>> stepAsync(
            ClaimedItem job,
            String name,
            Supplier<? extends CompletionStage<? extends T>> run,
            String toState,
            DurableStepOptions options,
            Executor closureExecutor,
            DurableResultDecoder<T> resultDecoder,
            boolean allowNullResult) {
        validateStep(job, name, run, toState);
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(closureExecutor, "closureExecutor");
        Objects.requireNonNull(resultDecoder, "resultDecoder");
        String valueName = valueName(name);
        return AsyncFutures.compose(
                preflightAsync(job, options),
                current -> {
                    String reference = committedReference(current, valueName);
                    if (reference != null) {
                        requireTargetState(current, toState);
                        return AsyncFutures.map(
                                storedResultAsync(reference, resultDecoder),
                                stored ->
                                        new DurableStepResult<>(
                                                claimFromRecord(current, job), stored, true));
                    }
                    return AsyncFutures.compose(
                            AsyncFutures.compose(
                                    AsyncFutures.dispatch(
                                            closureExecutor, run::get, "durable step closure"),
                                    stage ->
                                            Objects.requireNonNull(stage, "durable step stage")
                                                    .toCompletableFuture()),
                            value -> {
                                EncodedResult<T> encoded =
                                        encode(value, resultDecoder, allowNullResult);
                                return AsyncFutures.map(
                                        continueAsync(
                                                job,
                                                toState,
                                                new CommitValue(valueName, encoded.bytes()),
                                                options),
                                        refreshed ->
                                                new DurableStepResult<>(
                                                        refreshed, encoded.normalized(), false));
                            });
                });
    }

    static ClaimedItem claimFromRecord(FlowRecord record) {
        Objects.requireNonNull(record, "job");
        return new ClaimedItem(
                record.id(),
                record.leaseToken(),
                record.fencingToken(),
                record.partitionKey(),
                record.type(),
                record.state(),
                record.runState(),
                record.payload(),
                record.attributes());
    }

    private FlowRecord preflight(ClaimedItem job, DurableStepOptions options) {
        return AsyncFutures.await(
                preflightAsync(job, options),
                "durable step preflight was interrupted while waiting");
    }

    private CompletableFuture<FlowRecord> preflightAsync(
            ClaimedItem job, DurableStepOptions options) {
        List<Object> command =
                command(
                        "FLOW.EXTEND_LEASE",
                        job.id(),
                        job.leaseToken(),
                        "FENCING",
                        job.fencingToken(),
                        "LEASE_MS",
                        options.leaseMs(),
                        "NOW",
                        now(options));
        append(command, "PARTITION", job.partitionKey());
        return AsyncFutures.map(
                executeAsync(command),
                response -> {
                    FlowRecord current = Resp.optionalRecord(response, codec);
                    validatePreflight(job, current);
                    return current;
                });
    }

    private CompletableFuture<ClaimedItem> continueAsync(
            ClaimedItem job, String toState, CommitValue commit, DurableStepOptions options) {
        List<Object> command =
                command(
                        "FLOW.STEP_CONTINUE",
                        job.id(),
                        job.leaseToken(),
                        job.runState(),
                        toState,
                        "FENCING",
                        job.fencingToken(),
                        "LEASE_MS",
                        options.leaseMs(),
                        "NOW",
                        now(options));
        append(command, "PARTITION", job.partitionKey());
        if (commit != null) {
            command.add("VALUE");
            command.add(commit.name());
            command.add(commit.encoded());
        }
        command.add("RETURN");
        command.add("JOBS_COMPACT");

        CompletableFuture<ClaimedItem> result = new CompletableFuture<>();
        CompletableFuture<Object> request;
        try {
            request = executor.executeAsync(command);
        } catch (RuntimeException failure) {
            completeCommitFailure(result, failure);
            return result;
        }
        request.whenComplete(
                (response, failure) -> {
                    if (failure != null) {
                        completeCommitFailure(result, AsyncFutures.unwrap(failure));
                        return;
                    }
                    try {
                        validateResponseShape(response);
                        ClaimedItem refreshed = Resp.claimedItem(response);
                        validateRefreshed(job, refreshed, toState);
                        result.complete(mergeProjection(refreshed, job, toState));
                    } catch (RuntimeException failureToDecode) {
                        result.completeExceptionally(
                                unknown("decode FLOW.STEP_CONTINUE response", failureToDecode));
                    }
                });
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        request.cancel(false);
                    }
                });
        return result;
    }

    private <T> T storedResult(String reference, DurableResultDecoder<T> resultDecoder) {
        return AsyncFutures.await(
                storedResultAsync(reference, resultDecoder),
                "durable step result read was interrupted while waiting");
    }

    private <T> CompletableFuture<T> storedResultAsync(
            String reference, DurableResultDecoder<T> resultDecoder) {
        return AsyncFutures.map(
                executeAsync(command("FLOW.VALUE.MGET", reference)),
                response -> {
                    if (!(response instanceof List<?> values) || values.size() != 1) {
                        throw new FerricStoreException(
                                "committed durable step result must contain exactly one value");
                    }
                    Object value = values.get(0);
                    if (!(value instanceof byte[] bytes)) {
                        throw new FerricStoreException(
                                "committed durable step result is missing or invalid");
                    }
                    return resultDecoder.decode(bytes);
                });
    }

    private <T> EncodedResult<T> encode(
            Object value, DurableResultDecoder<T> resultDecoder, boolean allowNullResult) {
        if (value == null && !allowNullResult) {
            throw new FerricStoreException(
                    "a durable step returned null; use Void.class for an operation without a result");
        }
        byte[] bytes = codec.encode(value);
        if (bytes == null) {
            if (value != null) {
                throw new FerricStoreException("durable step codec returned a null encoded result");
            }
            bytes = codec instanceof JsonCodec ? bytes("null") : new byte[0];
        }
        T normalized = resultDecoder.decode(bytes);
        return new EncodedResult<>(bytes, normalized);
    }

    private <T> DurableResultDecoder<T> decoder(Class<T> resultType) {
        if (resultType == Void.class) {
            return ignored -> null;
        }
        return encoded -> codec.decode(encoded, resultType);
    }

    static String committedReference(FlowRecord record, String name) {
        Object rawReferences = record.raw().get("value_refs");
        if (rawReferences != null) {
            try {
                Resp.map(rawReferences);
            } catch (FerricStoreException malformed) {
                throw new FerricStoreException(
                        "FLOW.EXTEND_LEASE returned invalid value_refs", malformed);
            }
        }
        Map<String, Object> references = record.valueRefs();
        if (!references.containsKey(name)) {
            return null;
        }
        return reference(references.get(name));
    }

    static String reference(Object value) {
        if (value instanceof Map<?, ?> map) {
            value = map.get("ref");
            if (value == null) {
                value = map.get(bytes("ref"));
            }
        }
        String reference = text(value);
        if (reference == null || reference.isBlank()) {
            throw new FerricStoreException(
                    "committed durable step has an invalid result reference");
        }
        return reference;
    }

    private static void requireTargetState(FlowRecord current, String toState) {
        if (!toState.equals(current.runState())) {
            throw new FerricStoreException(
                    "committed durable step result does not match the requested target state");
        }
    }

    private static void validateStep(ClaimedItem job, String name, Object run, String toState) {
        validateClaim(job, toState);
        FlowValidation.requireText(name, "durable step name");
        Objects.requireNonNull(run, "run");
    }

    private static void validateClaim(ClaimedItem job, String toState) {
        Objects.requireNonNull(job, "job");
        FlowValidation.requireText(job.id(), "claimed workflow id");
        FlowValidation.requireText(job.leaseToken(), "claimed workflow lease token");
        FlowValidation.requireText(job.runState(), "claimed workflow run state");
        FlowValidation.requireText(toState, "target workflow state");
        if (job.fencingToken() <= 0) {
            throw new IllegalArgumentException("claimed workflow fencing token must be positive");
        }
        if (job.state() != null && !"running".equals(job.state())) {
            throw new IllegalArgumentException("claimed workflow state must be running");
        }
    }

    private static void validatePreflight(ClaimedItem job, FlowRecord current) {
        if (current == null) {
            throw new FerricStoreException("FLOW.EXTEND_LEASE returned no workflow record");
        }
        if (!Objects.equals(job.id(), current.id())
                || !Objects.equals(job.partitionKey(), current.partitionKey())) {
            throw new FerricStoreException(
                    "FLOW.EXTEND_LEASE returned a different workflow identity");
        }
        if (!Objects.equals(job.leaseToken(), current.leaseToken())
                || job.fencingToken() != current.fencingToken()) {
            throw new FerricStoreException("FLOW.EXTEND_LEASE returned a different workflow claim");
        }
        if (!"running".equals(current.state())
                || !Objects.equals(job.runState(), current.runState())) {
            throw new FerricStoreException("FLOW.EXTEND_LEASE returned a different workflow state");
        }
    }

    private static void validateRefreshed(
            ClaimedItem previous, ClaimedItem refreshed, String toState) {
        if (!Objects.equals(previous.id(), refreshed.id())
                || !Objects.equals(previous.partitionKey(), refreshed.partitionKey())) {
            throw new FerricStoreException(
                    "FLOW.STEP_CONTINUE returned a different workflow identity");
        }
        if (refreshed.leaseToken() == null
                || refreshed.leaseToken().isBlank()
                || refreshed.leaseToken().equals(previous.leaseToken())) {
            throw new FerricStoreException("FLOW.STEP_CONTINUE did not refresh the workflow lease");
        }
        if (refreshed.fencingToken() <= previous.fencingToken()) {
            throw new FerricStoreException(
                    "FLOW.STEP_CONTINUE did not increase the workflow fencing token");
        }
        if (!"running".equals(refreshed.state())
                || refreshed.runState() != null && !toState.equals(refreshed.runState())) {
            throw new FerricStoreException(
                    "FLOW.STEP_CONTINUE returned an unexpected workflow state");
        }
    }

    private static void validateResponseShape(Object response) {
        if (!(response instanceof Map<?, ?>)) {
            return;
        }
        Map<String, Object> fields = Resp.map(response);
        if (Resp.optionalString(fields.get("state")) == null
                || Resp.optionalString(fields.get("run_state")) == null) {
            throw new FerricStoreException(
                    "full FLOW.STEP_CONTINUE response is missing workflow state");
        }
    }

    private static ClaimedItem claimFromRecord(FlowRecord current, ClaimedItem fallback) {
        return mergeProjection(claimFromRecord(current), fallback, current.runState());
    }

    private static ClaimedItem mergeProjection(
            ClaimedItem refreshed, ClaimedItem fallback, String toState) {
        return new ClaimedItem(
                refreshed.id(),
                refreshed.leaseToken(),
                refreshed.fencingToken(),
                refreshed.partitionKey(),
                empty(refreshed.type()) ? fallback.type() : refreshed.type(),
                empty(refreshed.state()) ? "running" : refreshed.state(),
                empty(refreshed.runState()) ? toState : refreshed.runState(),
                refreshed.payload() == null ? fallback.payload() : refreshed.payload(),
                refreshed.attributes().isEmpty() ? fallback.attributes() : refreshed.attributes());
    }

    private static void completeCommitFailure(CompletableFuture<?> result, Throwable failure) {
        Throwable unwrapped = AsyncFutures.unwrap(failure);
        if (definitelyRejected(unwrapped)) {
            result.completeExceptionally(unwrapped);
        } else {
            result.completeExceptionally(unknown("FLOW.STEP_CONTINUE", unwrapped));
        }
    }

    private static boolean definitelyRejected(Throwable failure) {
        if (failure instanceof RequestDeliveryFailure deliveryFailure) {
            RequestDelivery delivery = deliveryFailure.delivery();
            return delivery == RequestDelivery.NOT_SENT || delivery == RequestDelivery.REJECTED;
        }
        return failure instanceof StaleLeaseException
                || failure instanceof FlowWrongStateException
                || failure instanceof FlowNotFoundException
                || failure instanceof InvalidCommandException;
    }

    private static DurableMutationOutcomeUnknownException unknown(
            String operation, Throwable cause) {
        if (cause instanceof DurableMutationOutcomeUnknownException existing) {
            return existing;
        }
        return new DurableMutationOutcomeUnknownException(operation, cause);
    }

    private static String valueName(String name) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(name.getBytes(StandardCharsets.UTF_8));
            return VALUE_PREFIX + java.util.HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is not available", failure);
        }
    }

    private static List<Object> command(Object... values) {
        return new ArrayList<>(List.of(values));
    }

    private static void append(List<Object> command, String option, Object value) {
        if (value != null) {
            command.add(option);
            command.add(value);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value == null ? null : String.valueOf(value);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    private CompletableFuture<Object> executeAsync(List<Object> command) {
        try {
            return executor.executeAsync(command);
        } catch (RuntimeException failure) {
            return AsyncFutures.failed(failure);
        }
    }

    private static long now(DurableStepOptions options) {
        return options.nowMs() == null ? System.currentTimeMillis() : options.nowMs();
    }

    private record CommitValue(String name, byte[] encoded) {}

    private record EncodedResult<T>(byte[] bytes, T normalized) {}
}
