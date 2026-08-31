package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class DurableStepTest {
    private static final String STEP_VALUE_NAME =
            "__ferricstore_step__:sha256:"
                    + "ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7";

    @Test
    void advanceInfersTheWholeClaimAndReturnsTheRefreshedClaim() {
        ScriptedExecutor commands =
                new ScriptedExecutor(List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        ClaimedItem refreshed = client.advance(claim(), "schedule_warning");

        assertEquals("flow-1", refreshed.id());
        assertEquals("tenant-1", refreshed.partitionKey());
        assertEquals("lease-2", refreshed.leaseToken());
        assertEquals(8L, refreshed.fencingToken());
        assertEquals("running", refreshed.state());
        assertEquals("schedule_warning", refreshed.runState());
        assertEquals("order", refreshed.type());
        assertEquals(Map.of("amount", 150), refreshed.payload());
        assertEquals(Map.of("region", "eu"), refreshed.attributes());

        List<Object> command = commands.calls().get(0);
        assertEquals("FLOW.STEP_CONTINUE", command.get(0));
        assertEquals("flow-1", command.get(1));
        assertEquals("lease-1", command.get(2));
        assertEquals("charge", command.get(3));
        assertEquals("schedule_warning", command.get(4));
        assertOption(command, "FENCING", 7L);
        assertOption(command, "PARTITION", "tenant-1");
        assertOption(command, "RETURN", "JOBS_COMPACT");
        assertFalse(command.contains("WORKER"));
    }

    @Test
    void advanceAcceptsTheFullClaimedFlowRecord() {
        ScriptedExecutor commands =
                new ScriptedExecutor(List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        ClaimedItem refreshed = client.advance(record(Map.of()), "schedule_warning");

        assertEquals("schedule_warning", refreshed.runState());
        assertEquals("lease-2", refreshed.leaseToken());
        assertEquals("charge", commands.calls().get(0).get(3));
    }

    @Test
    void stepRunsStoresAdvancesAndReturnsCodecNormalizedResult() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicInteger executions = new AtomicInteger();

        DurableStepResult<String> result =
                client.step(
                        claim(),
                        "charge-customer:v1",
                        () -> {
                            executions.incrementAndGet();
                            return new String(
                                    "provider-42".getBytes(StandardCharsets.UTF_8),
                                    StandardCharsets.UTF_8);
                        },
                        "schedule_warning",
                        String.class);

        assertEquals(1, executions.get());
        assertEquals("provider-42", result.result());
        assertFalse(result.replayed());
        assertEquals("lease-2", result.job().leaseToken());
        assertEquals("schedule_warning", result.job().runState());

        assertEquals("FLOW.EXTEND_LEASE", commands.calls().get(0).get(0));
        List<Object> commit = commands.calls().get(1);
        assertEquals("FLOW.STEP_CONTINUE", commit.get(0));
        assertOption(commit, "VALUE", STEP_VALUE_NAME);
        int valueIndex = commit.indexOf("VALUE");
        assertArrayEquals(bytes("provider-42"), (byte[]) commit.get(valueIndex + 2));
    }

    @Test
    void stepReplaysACommittedResultWithoutRunningTheClosure() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse(
                                "schedule_warning",
                                Map.of(STEP_VALUE_NAME, Map.of("ref", "value-ref-1"))),
                        List.of(bytes("provider-42")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicInteger executions = new AtomicInteger();

        DurableStepResult<String> result =
                client.step(
                        claim("schedule_warning", "lease-1", 7),
                        "charge-customer:v1",
                        () -> {
                            executions.incrementAndGet();
                            return "must-not-run";
                        },
                        "schedule_warning",
                        String.class);

        assertEquals(0, executions.get());
        assertEquals("provider-42", result.result());
        assertTrue(result.replayed());
        assertEquals("schedule_warning", result.job().runState());
        assertEquals(List.of("FLOW.EXTEND_LEASE", "FLOW.VALUE.MGET"), commands.commandNames());
    }

    @Test
    void typedStepDecodesDefaultRawCodecStringsIdenticallyOnFirstRunAndReplay() {
        FerricStoreClient firstClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                recordResponse("charge", Map.of()),
                                List.of("flow-1", "tenant-1", "lease-2", 8L)));

        DurableStepResult<String> first =
                firstClient.step(
                        claim(),
                        "charge-customer:v1",
                        () -> "provider-42",
                        "schedule_warning",
                        String.class);

        FerricStoreClient replayClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                recordResponse(
                                        "schedule_warning",
                                        Map.of(STEP_VALUE_NAME, "value-ref-1"),
                                        "lease-2",
                                        8L),
                                List.of(bytes("provider-42"))));
        DurableStepResult<String> replay =
                replayClient.step(
                        claim("schedule_warning", "lease-2", 8),
                        "charge-customer:v1",
                        () -> "must-not-run",
                        "schedule_warning",
                        String.class);

        assertEquals("provider-42", first.result());
        assertEquals("provider-42", replay.result());
        assertFalse(first.replayed());
        assertTrue(replay.replayed());
    }

    @Test
    void typedJsonPojoIsIdenticalAcrossAsyncFirstRunAndReplay() {
        Receipt expected = new Receipt("provider-42", 150);
        FerricStoreClient firstClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                jsonRecordResponse("charge", Map.of(), "lease-1", 7L),
                                List.of("flow-1", "tenant-1", "lease-2", 8L)),
                        new JsonCodec());

        DurableStepResult<Receipt> first =
                firstClient
                        .stepAsync(
                                claim(),
                                "charge-customer:v1",
                                () -> CompletableFuture.completedFuture(expected),
                                "schedule_warning",
                                Receipt.class)
                        .join();

        FerricStoreClient replayClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                jsonRecordResponse(
                                        "schedule_warning",
                                        Map.of(STEP_VALUE_NAME, "value-ref-1"),
                                        "lease-2",
                                        8L),
                                List.of(new JsonCodec().encode(expected))),
                        new JsonCodec());
        DurableStepResult<Receipt> replay =
                replayClient
                        .stepAsync(
                                claim("schedule_warning", "lease-2", 8),
                                "charge-customer:v1",
                                () -> CompletableFuture.completedFuture(new Receipt("wrong", 0)),
                                "schedule_warning",
                                Receipt.class)
                        .join();

        assertEquals(expected, first.result());
        assertEquals(expected, replay.result());
        assertFalse(first.replayed());
        assertTrue(replay.replayed());
    }

    @Test
    void typedVoidResultCommitsAndReplaysWithoutRerunningTheClosure() {
        ScriptedExecutor firstCommands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient firstClient = FerricStoreClient.fromExecutor(firstCommands);

        DurableStepResult<Void> first =
                firstClient.step(
                        claim(), "charge-customer:v1", () -> null, "schedule_warning", Void.class);

        AtomicInteger replayExecutions = new AtomicInteger();
        FerricStoreClient replayClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                recordResponse(
                                        "schedule_warning",
                                        Map.of(STEP_VALUE_NAME, "value-ref-1"),
                                        "lease-2",
                                        8L),
                                List.of(new byte[0])));
        DurableStepResult<Void> replay =
                replayClient.step(
                        claim("schedule_warning", "lease-2", 8),
                        "charge-customer:v1",
                        () -> {
                            replayExecutions.incrementAndGet();
                            return null;
                        },
                        "schedule_warning",
                        Void.class);

        assertEquals(null, first.result());
        assertEquals(null, replay.result());
        assertEquals(0, replayExecutions.get());
        List<Object> commit = firstCommands.calls().get(1);
        assertArrayEquals(new byte[0], (byte[]) commit.get(commit.indexOf("VALUE") + 2));
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void nonVoidNullResultsAreRejectedWithoutConflatingThemWithEmptyValues() {
        for (Class<?> type : List.of(String.class, byte[].class)) {
            FerricStoreClient client =
                    FerricStoreClient.fromExecutor(
                            new ScriptedExecutor(recordResponse("charge", Map.of())));

            FerricStoreException failure =
                    assertThrows(
                            FerricStoreException.class,
                            () ->
                                    client.step(
                                            claim(),
                                            "charge-customer:v1",
                                            () -> null,
                                            "schedule_warning",
                                            type));

            assertTrue(failure.getMessage().contains("Void.class"));
        }

        DurableStepResult<String> emptyString =
                FerricStoreClient.fromExecutor(
                                new ScriptedExecutor(
                                        recordResponse("charge", Map.of()),
                                        List.of("flow-1", "tenant-1", "lease-2", 8L)))
                        .step(
                                claim(),
                                "charge-customer:v1",
                                () -> "",
                                "schedule_warning",
                                String.class);
        DurableStepResult<byte[]> emptyBytes =
                FerricStoreClient.fromExecutor(
                                new ScriptedExecutor(
                                        recordResponse("charge", Map.of()),
                                        List.of("flow-1", "tenant-1", "lease-2", 8L)))
                        .step(
                                claim(),
                                "charge-customer:v1",
                                () -> new byte[0],
                                "schedule_warning",
                                byte[].class);

        assertEquals("", emptyString.result());
        assertArrayEquals(new byte[0], emptyBytes.result());
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void malformedValueReferenceContainersFailClosedBeforeTheClosureRuns() {
        for (Object malformed : List.of("not-a-map", List.of("orphan-key"))) {
            Map<String, Object> response = recordResponse("charge", Map.of());
            response.put("value_refs", malformed);
            AtomicInteger executions = new AtomicInteger();
            FerricStoreClient client =
                    FerricStoreClient.fromExecutor(
                            new ScriptedExecutor(response), new StringCodec());

            FerricStoreException failure =
                    assertThrows(
                            FerricStoreException.class,
                            () ->
                                    client.step(
                                            claim(),
                                            "charge-customer:v1",
                                            () -> {
                                                executions.incrementAndGet();
                                                return "must-not-run";
                                            },
                                            "schedule_warning"));

            assertTrue(failure.getMessage().contains("value_refs"));
            assertEquals(0, executions.get());
        }
    }

    @Test
    void committedResultMustMatchTheRequestedTargetState() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse(
                                "different-state",
                                Map.of(STEP_VALUE_NAME, Map.of("ref", "value-ref-1"))));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        FerricStoreException error =
                assertThrows(
                        FerricStoreException.class,
                        () ->
                                client.step(
                                        claim("different-state", "lease-1", 7),
                                        "charge-customer:v1",
                                        () -> "must-not-run",
                                        "schedule_warning"));

        assertTrue(error.getMessage().contains("target state"));
        assertEquals(List.of("FLOW.EXTEND_LEASE"), commands.commandNames());
    }

    @Test
    void committedResultReferencesAndValuesFailClosed() {
        FerricStoreClient invalidReference =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                recordResponse(
                                        "schedule_warning", Map.of(STEP_VALUE_NAME, Map.of()))),
                        new StringCodec());
        FerricStoreException refError =
                assertThrows(
                        FerricStoreException.class,
                        () ->
                                invalidReference.step(
                                        claim("schedule_warning", "lease-1", 7),
                                        "charge-customer:v1",
                                        () -> "must-not-run",
                                        "schedule_warning"));
        assertTrue(refError.getMessage().contains("reference"));

        FerricStoreClient missingValue =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(
                                recordResponse(
                                        "schedule_warning", Map.of(STEP_VALUE_NAME, "value-ref-1")),
                                java.util.Arrays.asList((Object) null)),
                        new StringCodec());
        FerricStoreException valueError =
                assertThrows(
                        FerricStoreException.class,
                        () ->
                                missingValue.step(
                                        claim("schedule_warning", "lease-1", 7),
                                        "charge-customer:v1",
                                        () -> "must-not-run",
                                        "schedule_warning"));
        assertTrue(valueError.getMessage().contains("missing"));
    }

    @Test
    void malformedOrUnrefreshedCommitResponsesAreOutcomeUnknown() {
        for (Object response :
                List.of(
                        "OK",
                        List.of("flow-1", "tenant-1", "lease-1", 8L),
                        List.of("flow-1", "tenant-1", "lease-2", 7L),
                        refreshedResponse(null, "schedule_warning"),
                        refreshedResponse("running", null))) {
            assertCommitResponseIsOutcomeUnknown(response);
        }
    }

    private static void assertCommitResponseIsOutcomeUnknown(Object response) {
        FerricStoreClient client =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(recordResponse("charge", Map.of()), response),
                        new StringCodec());

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () ->
                        client.step(
                                claim(),
                                "charge-customer:v1",
                                () -> "provider-42",
                                "schedule_warning"));
    }

    private static Map<String, Object> refreshedResponse(String state, String runState) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", "flow-1");
        response.put("partition_key", "tenant-1");
        response.put("lease_token", "lease-2");
        response.put("fencing_token", 8L);
        if (state != null) {
            response.put("state", state);
        }
        if (runState != null) {
            response.put("run_state", runState);
        }
        return response;
    }

    @Test
    void commitTransportFailureIsUnknownButServerRejectionIsNotWrapped() {
        ScriptedExecutor uncertain =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        new NativeProtocolException("connection failed after send"));
        FerricStoreClient uncertainClient =
                FerricStoreClient.fromExecutor(uncertain, new StringCodec());

        assertThrows(
                DurableMutationOutcomeUnknownException.class,
                () ->
                        uncertainClient.step(
                                claim(),
                                "charge-customer:v1",
                                () -> "provider-42",
                                "schedule_warning"));

        NativeServerException rejection =
                new NativeServerException(
                        NativeProtocol.STATUS_ERROR,
                        Map.of(
                                "code",
                                "stale_lease",
                                "message",
                                "stale lease",
                                "safe_to_retry",
                                false));
        FerricStoreClient rejectedClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(recordResponse("charge", Map.of()), rejection),
                        new StringCodec());

        assertThrows(
                NativeServerException.class,
                () ->
                        rejectedClient.step(
                                claim(),
                                "charge-customer:v1",
                                () -> "provider-42",
                                "schedule_warning"));

        InvalidCommandException invalidCommand =
                new InvalidCommandException("commit command is invalid");
        FerricStoreClient invalidCommandClient =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(recordResponse("charge", Map.of()), invalidCommand),
                        new StringCodec());

        InvalidCommandException actual =
                assertThrows(
                        InvalidCommandException.class,
                        () ->
                                invalidCommandClient.step(
                                        claim(),
                                        "charge-customer:v1",
                                        () -> "provider-42",
                                        "schedule_warning"));
        assertSame(invalidCommand, actual);
    }

    @Test
    void asyncAdvanceAndStepHaveTheSameContract() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        List.of("flow-1", "tenant-1", "lease-2", 8L),
                        recordResponse("schedule_warning", Map.of(), "lease-2", 8L),
                        List.of("flow-1", "tenant-1", "lease-3", 9L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        ClaimedItem advanced = client.advanceAsync(claim(), "schedule_warning").join();
        DurableStepResult<String> stepped =
                client.stepAsync(
                                advanced,
                                "notify-customer:v1",
                                () -> CompletableFuture.completedFuture("notification-1"),
                                "done",
                                String.class)
                        .join();

        assertEquals("schedule_warning", advanced.runState());
        assertEquals("done", stepped.job().runState());
        assertEquals("notification-1", stepped.result());
        assertFalse(stepped.replayed());
    }

    @Test
    void asyncUnknownCommitCompletesExceptionallyWithTheDedicatedType() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        new NativeProtocolException("connection failed after send"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());

        CompletionException error =
                assertThrows(
                        CompletionException.class,
                        () ->
                                client.stepAsync(
                                                claim(),
                                                "charge-customer:v1",
                                                () ->
                                                        CompletableFuture.completedFuture(
                                                                "provider-42"),
                                                "schedule_warning")
                                        .join());

        assertInstanceOf(DurableMutationOutcomeUnknownException.class, error.getCause());
    }

    @Test
    void asyncClosureRunsOnTheCallerOwnedExecutorInsteadOfATransportCompletionThread() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicReference<String> closureThread = new AtomicReference<>();
        ExecutorService executor =
                Executors.newSingleThreadExecutor(task -> new Thread(task, "durable-closure"));
        try {
            DurableStepResult<String> result =
                    client.stepAsync(
                                    claim(),
                                    "charge-customer:v1",
                                    () -> {
                                        closureThread.set(Thread.currentThread().getName());
                                        return CompletableFuture.completedFuture("provider-42");
                                    },
                                    "schedule_warning",
                                    String.class,
                                    DurableStepOptions.defaults(),
                                    executor)
                            .join();

            assertEquals("provider-42", result.result());
            assertEquals("durable-closure", closureThread.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void asyncClosureRejectsADirectExecutorBeforeRunningApplicationCodeInline() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicInteger executions = new AtomicInteger();

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                client.stepAsync(
                                                claim(),
                                                "charge-customer:v1",
                                                () -> {
                                                    executions.incrementAndGet();
                                                    return CompletableFuture.completedFuture(
                                                            "must-not-run-inline");
                                                },
                                                "schedule_warning",
                                                String.class,
                                                DurableStepOptions.defaults(),
                                                Runnable::run)
                                        .join());

        assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        assertEquals(0, executions.get());
        assertEquals(List.of("FLOW.EXTEND_LEASE"), commands.commandNames());
    }

    @Test
    void asyncClosureRejectsCallerRunsSaturationBeforeRunningApplicationCodeInline()
            throws Exception {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
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
        AtomicInteger executions = new AtomicInteger();
        try {
            CompletionException failure =
                    assertThrows(
                            CompletionException.class,
                            () ->
                                    client.stepAsync(
                                                    claim(),
                                                    "charge-customer:v1",
                                                    () -> {
                                                        executions.incrementAndGet();
                                                        return CompletableFuture.completedFuture(
                                                                "must-not-run-inline");
                                                    },
                                                    "schedule_warning",
                                                    String.class,
                                                    DurableStepOptions.defaults(),
                                                    executor)
                                            .join());

            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            assertEquals(0, executions.get());
            assertEquals(List.of("FLOW.EXTEND_LEASE"), commands.commandNames());
        } finally {
            release.countDown();
            blocker.get(2, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }

    @Test
    void asyncCustomDecoderAcceptsTimingOptionsWithoutRequiringAnExecutor() {
        ScriptedExecutor commands =
                new ScriptedExecutor(
                        recordResponse("charge", Map.of()),
                        List.of("flow-1", "tenant-1", "lease-2", 8L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        DurableStepOptions options = DurableStepOptions.builder().leaseMs(1_234).nowMs(42).build();

        DurableStepResult<String> result =
                client.stepAsync(
                                claim(),
                                "charge-customer:v1",
                                () -> CompletableFuture.completedFuture("provider-42"),
                                "schedule_warning",
                                encoded ->
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .toUpperCase(java.util.Locale.ROOT),
                                options)
                        .join();

        assertEquals("PROVIDER-42", result.result());
        assertOption(commands.calls().get(0), "LEASE_MS", 1_234L);
        assertOption(commands.calls().get(0), "NOW", 42L);
        assertOption(commands.calls().get(1), "LEASE_MS", 1_234L);
        assertOption(commands.calls().get(1), "NOW", 42L);
    }

    @Test
    void requestDeliveryMetadataSeparatesNotSentRejectedAndUnknownFailures() {
        assertEquals(
                RequestDelivery.NOT_SENT,
                NativeProtocolException.notSent("closed before send").delivery());
        assertEquals(
                RequestDelivery.UNKNOWN,
                new NativeProtocolException("failed after send").delivery());
        assertEquals(
                RequestDelivery.REJECTED,
                new NativeServerException(
                                NativeProtocol.STATUS_BAD_REQUEST, Map.of("message", "bad"))
                        .delivery());

        HttpTransportException capacity =
                new HttpTransportException(
                        "capacity",
                        0,
                        "client_overloaded",
                        false,
                        false,
                        null,
                        Map.of(),
                        null,
                        RequestDelivery.NOT_SENT);
        assertEquals(RequestDelivery.NOT_SENT, capacity.delivery());
        HttpTransportException serverFailure =
                new HttpTransportException(
                        "server", 503, "server_overloaded", true, false, null, Map.of(), null);
        assertEquals(RequestDelivery.UNKNOWN, serverFailure.delivery());
        HttpTransportException dispatchedTimeout =
                new HttpTransportException(
                        "request timed out after dispatch",
                        408,
                        "request_timeout",
                        true,
                        false,
                        null,
                        Map.of(),
                        null);
        assertEquals(RequestDelivery.UNKNOWN, dispatchedTimeout.delivery());
    }

    @Test
    void definitelyNotSentCommitFailureIsNotMisreportedAsOutcomeUnknown() {
        NativeProtocolException notSent =
                NativeProtocolException.notSent("native pending request limit exceeded");
        FerricStoreClient client =
                FerricStoreClient.fromExecutor(
                        new ScriptedExecutor(recordResponse("charge", Map.of()), notSent),
                        new StringCodec());

        NativeProtocolException error =
                assertThrows(
                        NativeProtocolException.class,
                        () ->
                                client.step(
                                        claim(),
                                        "charge-customer:v1",
                                        () -> "provider-42",
                                        "schedule_warning"));

        assertEquals(RequestDelivery.NOT_SENT, error.delivery());
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void unclassifiedCommitFailuresFailClosedAsOutcomeUnknown() {
        for (RuntimeException failure :
                List.of(
                        new IllegalArgumentException("custom executor failed after commit"),
                        new NullDeliveryException(),
                        new NativeServerException(
                                NativeProtocol.STATUS_ERROR,
                                Map.of("code", "timeout", "message", "Raft outcome unknown")),
                        new HttpTransportException(
                                "request timed out after dispatch",
                                408,
                                "request_timeout",
                                true,
                                false,
                                null,
                                Map.of(),
                                null),
                        new HttpCommandException(
                                "Raft outcome unknown", "timeout", true, false, null, Map.of()))) {
            FerricStoreClient client =
                    FerricStoreClient.fromExecutor(
                            new ScriptedExecutor(recordResponse("charge", Map.of()), failure),
                            new StringCodec());

            assertThrows(
                    DurableMutationOutcomeUnknownException.class,
                    () ->
                            client.step(
                                    claim(),
                                    "charge-customer:v1",
                                    () -> "provider-42",
                                    "schedule_warning"));
        }
    }

    @Test
    void validatesClaimsAndStableNamesBeforeAnyCommandOrClosure() {
        ScriptedExecutor commands = new ScriptedExecutor();
        FerricStoreClient client = FerricStoreClient.fromExecutor(commands, new StringCodec());
        AtomicInteger executions = new AtomicInteger();

        ClaimedItem invalidFence =
                new ClaimedItem(
                        "flow-1",
                        "lease-1",
                        0,
                        "tenant-1",
                        "order",
                        "running",
                        "charge",
                        null,
                        Map.of());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        client.step(
                                invalidFence,
                                "charge-customer:v1",
                                () -> {
                                    executions.incrementAndGet();
                                    return "provider-42";
                                },
                                "schedule_warning"));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        client.step(
                                claim(),
                                " ",
                                () -> {
                                    executions.incrementAndGet();
                                    return "provider-42";
                                },
                                "schedule_warning"));

        assertEquals(0, executions.get());
        assertTrue(commands.calls().isEmpty());
    }

    private static ClaimedItem claim() {
        return claim("charge", "lease-1", 7);
    }

    private static ClaimedItem claim(String runState, String leaseToken, long fencingToken) {
        return new ClaimedItem(
                "flow-1",
                leaseToken,
                fencingToken,
                "tenant-1",
                "order",
                "running",
                runState,
                Map.of("amount", 150),
                Map.of("region", "eu"));
    }

    private static FlowRecord record(Map<String, Object> refs) {
        return Resp.record(recordResponse("charge", refs), new StringCodec());
    }

    private static Map<String, Object> recordResponse(String runState, Map<String, Object> refs) {
        return recordResponse(runState, refs, "lease-1", 7L);
    }

    private static Map<String, Object> recordResponse(
            String runState, Map<String, Object> refs, String leaseToken, long fencingToken) {
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
        response.put("value_refs", refs);
        response.put("attributes", Map.of("region", "eu"));
        return response;
    }

    private static Map<String, Object> jsonRecordResponse(
            String runState, Map<String, Object> refs, String leaseToken, long fencingToken) {
        Map<String, Object> response = recordResponse(runState, refs, leaseToken, fencingToken);
        response.put("payload", null);
        return response;
    }

    private static void assertOption(List<Object> command, String name, Object expected) {
        int index = command.indexOf(name);
        assertTrue(index >= 0, () -> "missing option " + name + " in " + command);
        assertEquals(expected, command.get(index + 1));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class ScriptedExecutor implements CommandExecutor {
        private final Deque<Object> responses = new ArrayDeque<>();
        private final List<List<Object>> calls = new ArrayList<>();

        private ScriptedExecutor(Object... responses) {
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
    }

    private static final class NullDeliveryException extends RuntimeException
            implements RequestDeliveryFailure {
        private static final long serialVersionUID = 1L;

        @Override
        public RequestDelivery delivery() {
            return null;
        }
    }

    private record Receipt(String id, int amount) {}
}
