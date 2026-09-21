package com.ferricstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/** Multiplexed TCP/TLS executor for FerricStore's native protocol v1. */
public final class NativeExecutor implements SessionCommandExecutor, SessionExecutorFactory {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int DATA_LANES = 32;
    private static final int MAX_PIPELINE_COMMANDS = 1_024;
    private static final int MAX_REQUEST_BYTES = 64 * 1024 * 1024;
    private static final int MAX_BUFFERED_EVENTS = 1_024;
    private static final Object CLOSED_EVENT = new Object();
    private static final String GOAWAY_MESSAGE = "native connection is draining after GOAWAY";
    private static final Set<String> DEDICATED_SESSION_COMMANDS =
            Set.of(
                    "AUTH",
                    "DISCARD",
                    "EXEC",
                    "HELLO",
                    "MULTI",
                    "PSUBSCRIBE",
                    "PUNSUBSCRIBE",
                    "QUIT",
                    "RESET",
                    "SELECT",
                    "SUBSCRIBE",
                    "UNSUBSCRIBE",
                    "UNWATCH",
                    "WATCH");

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;
    private final Object writeLock = new Object();
    private final ExecutorService writeExecutor;
    private final ExecutorService reconnectExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final AtomicLong requestIds = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean transportRetired = new AtomicBoolean();
    private final AtomicReference<RuntimeException> terminationFailure = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<NativeExecutor>> replacement =
            new AtomicReference<>();
    private final CountDownLatch dataWriteStarted = new CountDownLatch(1);
    private final ConcurrentHashMap<Long, PendingRequest> pending = new ConcurrentHashMap<>();
    private final Set<RetryOperation> retryOperations = ConcurrentHashMap.newKeySet();
    private final NativeResponseAssembler assembler =
            new NativeResponseAssembler(
                    NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES,
                    NativeProtocol.DEFAULT_MAX_RESPONSE_CHUNKS);
    private final NativeEndpoint endpoint;
    private final NativeTransportOptions transportOptions;
    private final Semaphore pendingSlots;
    private final LongSupplier nanoTime;
    private final Duration requestTimeout;
    private final boolean dedicatedSession;
    private final Thread readerThread;
    private final BlockingQueue<Object> events = new ArrayBlockingQueue<>(MAX_BUFFERED_EVENTS);
    private final AtomicReference<RuntimeException> eventFailure = new AtomicReference<>();

    private final AtomicInteger maxFrameBytes =
            new AtomicInteger(NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES);
    private final AtomicReference<NegotiatedCapabilities> negotiatedCapabilities =
            new AtomicReference<>();
    private final AtomicBoolean authenticated = new AtomicBoolean();
    private final Set<SessionOpening> sessionOpenings = new HashSet<>();

    private NativeExecutor(
            NativeEndpoint endpoint,
            NativeTransportOptions transportOptions,
            boolean dedicatedSession)
            throws IOException {
        this(endpoint, transportOptions, dedicatedSession, System::nanoTime, REQUEST_TIMEOUT);
    }

    private NativeExecutor(
            NativeEndpoint endpoint,
            NativeTransportOptions transportOptions,
            boolean dedicatedSession,
            LongSupplier nanoTime)
            throws IOException {
        this(endpoint, transportOptions, dedicatedSession, nanoTime, REQUEST_TIMEOUT);
    }

    private NativeExecutor(
            NativeEndpoint endpoint,
            NativeTransportOptions transportOptions,
            boolean dedicatedSession,
            LongSupplier nanoTime,
            Duration requestTimeout)
            throws IOException {
        this(endpoint, transportOptions, dedicatedSession, nanoTime, requestTimeout, null);
    }

    private NativeExecutor(
            NativeEndpoint endpoint,
            NativeTransportOptions transportOptions,
            boolean dedicatedSession,
            LongSupplier nanoTime,
            Duration requestTimeout,
            SessionOpening opening)
            throws IOException {
        this.endpoint = endpoint;
        this.transportOptions = transportOptions;
        this.pendingSlots = new Semaphore(transportOptions.maxPendingRequests());
        this.nanoTime = Objects.requireNonNull(nanoTime, "nano time");
        this.requestTimeout = requirePositiveTimeout(requestTimeout);
        this.dedicatedSession = dedicatedSession;
        Socket connected = connectSocket(endpoint, transportOptions.sslContext(), opening);
        this.socket = connected;
        try {
            this.input = connected.getInputStream();
            this.output = connected.getOutputStream();
        } catch (IOException | RuntimeException error) {
            try {
                connected.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
        this.writeExecutor = Executors.newSingleThreadExecutor(NativeExecutor::writerThread);
        this.reconnectExecutor = Executors.newSingleThreadExecutor(NativeExecutor::reconnectThread);
        this.retryExecutor = retryExecutor();
        this.readerThread = new Thread(() -> readLoop(this.input), "ferricstore-native-reader");
        this.readerThread.setDaemon(true);
        if (opening != null) {
            opening.attachExecutor(this);
        }
        this.readerThread.start();
    }

    public static NativeExecutor connect(String uri) {
        return connectWithOptions(uri, NativeTransportOptions.defaults());
    }

    /** Connects with an optional caller-provided TLS context for {@code ferrics://} URLs. */
    public static NativeExecutor connect(String uri, SSLContext sslContext) {
        NativeTransportOptions.Builder options = NativeTransportOptions.builder();
        if (sslContext != null) {
            options.sslContext(sslContext);
        }
        return connectWithOptions(uri, options.build());
    }

    /** Connects with caller-provided native transport limits and TLS settings. */
    public static NativeExecutor connectWithOptions(
            String uri, NativeTransportOptions transportOptions) {
        return connectWithOptions(uri, transportOptions, System::nanoTime);
    }

    static NativeExecutor connectWithOptions(
            String uri, NativeTransportOptions transportOptions, LongSupplier nanoTime) {
        return connectWithOptions(uri, transportOptions, nanoTime, REQUEST_TIMEOUT);
    }

    static NativeExecutor connectWithOptions(
            String uri,
            NativeTransportOptions transportOptions,
            LongSupplier nanoTime,
            Duration requestTimeout) {
        Objects.requireNonNull(transportOptions, "native transport options");
        NativeEndpoint endpoint = NativeEndpoint.parse(uri);
        try {
            NativeExecutor executor =
                    new NativeExecutor(endpoint, transportOptions, false, nanoTime, requestTimeout);
            try {
                executor.initialize();
                return executor;
            } catch (RuntimeException error) {
                executor.close();
                throw error;
            }
        } catch (IOException error) {
            throw new NativeProtocolException(
                    "failed to connect to FerricStore native endpoint", error);
        }
    }

    public NegotiatedCapabilities negotiatedCapabilities() {
        NegotiatedCapabilities current = negotiatedCapabilities.get();
        if (current == null) {
            throw new IllegalStateException("FerricStore HELLO negotiation is not complete");
        }
        return current;
    }

    void runWithWriteLockForTesting(Runnable action) {
        Objects.requireNonNull(action, "action");
        synchronized (writeLock) {
            action.run();
        }
    }

    boolean awaitDataWriteStartedForTesting(Duration timeout) {
        try {
            return dataWriteStarted.await(durationToNanos(timeout), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    void setSendBufferSizeForTesting(int bytes) throws IOException {
        socket.setSendBufferSize(bytes);
    }

    @Override
    public SessionCommandExecutor openSession() {
        SessionOpening opening = beginSessionOpening();
        NativeExecutor session = null;
        boolean published = false;
        try {
            session =
                    new NativeExecutor(
                            endpoint, transportOptions, true, nanoTime, requestTimeout, opening);
            session.initialize();
            if (!publishSessionOpening(opening)) {
                throw NativeProtocolException.notSent("native connection is closed");
            }
            published = true;
            return session;
        } catch (IOException error) {
            if (closed.get() || opening.cancelled()) {
                throw NativeProtocolException.notSent("native connection is closed", error);
            }
            throw new NativeProtocolException(
                    "failed to open dedicated FerricStore native session", error);
        } catch (RuntimeException error) {
            if (closed.get() || opening.cancelled()) {
                throw NativeProtocolException.notSent("native connection is closed", error);
            }
            throw error;
        } finally {
            if (!published) {
                removeSessionOpening(opening);
                if (session != null) {
                    closeUnpublishedSession(session);
                }
            }
        }
    }

    private static void closeUnpublishedSession(NativeExecutor session) {
        if (!session.closed.get()) {
            session.close();
        }
    }

    private SessionOpening beginSessionOpening() {
        synchronized (sessionOpenings) {
            if (closed.get()) {
                throw NativeProtocolException.notSent("native connection is closed");
            }
            SessionOpening opening = new SessionOpening();
            sessionOpenings.add(opening);
            return opening;
        }
    }

    private boolean publishSessionOpening(SessionOpening opening) {
        synchronized (sessionOpenings) {
            if (closed.get() || opening.cancelled()) {
                return false;
            }
            sessionOpenings.remove(opening);
            return true;
        }
    }

    private void removeSessionOpening(SessionOpening opening) {
        synchronized (sessionOpenings) {
            sessionOpenings.remove(opening);
        }
    }

    private void cancelSessionOpenings() {
        SessionOpening[] openings;
        synchronized (sessionOpenings) {
            openings = sessionOpenings.toArray(SessionOpening[]::new);
        }
        for (SessionOpening opening : openings) {
            opening.cancel();
        }
    }

    @Override
    public Object execute(List<Object> args) {
        return AsyncFutures.await(
                executeAsync(args),
                error ->
                        new NativeProtocolException(
                                "native request was interrupted after sending; outcome is unknown",
                                error));
    }

    @Override
    public CompletableFuture<Object> executeAsync(List<Object> args) {
        PreparedCommand prepared;
        try {
            prepared = prepareCommand(args, true);
        } catch (RuntimeException failure) {
            return AsyncFutures.failed(notSent("failed to prepare native command", failure));
        }
        if (prepared.flags() != 0) {
            return requestWithRetryAsync(
                    prepared.opcode(),
                    prepared.laneId(),
                    (byte[]) prepared.payload(),
                    prepared.flags());
        }
        return requestWithRetryAsync(prepared.opcode(), prepared.laneId(), prepared.payload());
    }

    @Override
    public CompletableFuture<List<Object>> pipelineAsync(List<List<Object>> commands) {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        List<PreparedPipelineBatch> batches = new ArrayList<>();
        try {
            for (int start = 0; start < commands.size(); start += MAX_PIPELINE_COMMANDS) {
                int end = Math.min(start + MAX_PIPELINE_COMMANDS, commands.size());
                batches.add(preparePipelineBatch(commands.subList(start, end)));
            }
        } catch (RuntimeException failure) {
            return AsyncFutures.failed(notSent("failed to prepare native pipeline", failure));
        }
        List<Object> results = new ArrayList<>(commands.size());
        CompletableFuture<Void> sequence = CompletableFuture.completedFuture(null);
        for (PreparedPipelineBatch batch : batches) {
            sequence =
                    AsyncFutures.compose(
                            sequence,
                            ignored ->
                                    AsyncFutures.map(
                                            executePipelineBatch(batch),
                                            values -> {
                                                results.addAll(values);
                                                return null;
                                            }));
        }
        return AsyncFutures.map(
                sequence,
                ignored -> java.util.Collections.unmodifiableList(new ArrayList<>(results)));
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private PreparedPipelineBatch preparePipelineBatch(List<List<Object>> commands) {
        FlowCreatePipeline.Batch flowCreateBatch = FlowCreatePipeline.tryParse(commands);
        NativeFlowPipelineCodec.Encoded flowCreateMany =
                NativeFlowPipelineCodec.tryEncodeCreateMany(flowCreateBatch);
        if (flowCreateMany != null) {
            return preparedPipelineBatch(
                    NativeProtocol.OP_FLOW_CREATE_MANY,
                    laneFor(commands.get(0)),
                    flowCreateMany.payload(),
                    NativeProtocol.FLAG_CUSTOM_PAYLOAD,
                    commands.size(),
                    PipelineResponseType.FLOW_MANY);
        }
        if (flowCreateBatch != null) {
            return preparedPipelineBatch(
                    NativeProtocol.OP_FLOW_CREATE_MANY,
                    laneFor(commands.get(0)),
                    encodeRequestBody(flowCreateBatch.typedPayload()),
                    0,
                    commands.size(),
                    PipelineResponseType.FLOW_MANY);
        }
        NativePipelineCodec.Encoded compact = NativePipelineCodec.tryEncodeDetailed(commands);
        if (compact != null) {
            return preparedPipelineBatch(
                    NativeProtocol.OP_PIPELINE,
                    laneFor(commands.get(0)),
                    compact.payload(),
                    NativeProtocol.FLAG_CUSTOM_PAYLOAD,
                    commands.size(),
                    PipelineResponseType.PIPELINE);
        }

        List<Object> encodedCommands = new ArrayList<>(commands.size());
        long outerLane = 1;
        for (int index = 0; index < commands.size(); index++) {
            PreparedCommand prepared = prepareCommand(commands.get(index), false);
            if (index == 0) {
                outerLane = prepared.laneId();
            }
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("opcode", prepared.opcode());
            encoded.put("lane_id", prepared.laneId());
            encoded.put("request_id", index + 1L);
            encoded.put("body", prepared.payload());
            encodedCommands.add(encoded);
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("atomicity", "none");
        payload.put("commands", encodedCommands);
        payload.put("return", "pairs");
        return preparedPipelineBatch(
                NativeProtocol.OP_PIPELINE,
                outerLane,
                encodeRequestBody(payload),
                0,
                commands.size(),
                PipelineResponseType.PIPELINE);
    }

    private PreparedPipelineBatch preparedPipelineBatch(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int expected,
            PipelineResponseType responseType) {
        validateRequestBody(body);
        return new PreparedPipelineBatch(opcode, laneId, body, flags, expected, responseType);
    }

    private byte[] encodeRequestBody(Object payload) {
        return NativeValueCodec.encode(payload, MAX_REQUEST_BYTES);
    }

    private CompletableFuture<List<Object>> executePipelineBatch(PreparedPipelineBatch batch) {
        long deadlineNanos = deadlineAfter(requestTimeout);
        return AsyncFutures.map(
                requestWithRetryAsync(
                        batch.opcode(), batch.laneId(), batch.body(), batch.flags(), deadlineNanos),
                value -> mapPipelineResponse(value, batch, deadlineNanos));
    }

    private List<Object> mapPipelineResponse(
            Object value, PreparedPipelineBatch batch, long deadlineNanos) {
        try {
            List<Object> results =
                    switch (batch.responseType()) {
                        case PIPELINE -> requirePipelineResults(value, batch.expected());
                        case FLOW_MANY -> requireFlowManyResults(value, batch.expected());
                    };
            if (deadlineExpired(deadlineNanos)) {
                throw requestTimeout();
            }
            return results;
        } catch (RuntimeException error) {
            if (deadlineExpired(deadlineNanos)) {
                throw requestTimeout(error);
            }
            throw error;
        }
    }

    private PreparedCommand prepareCommand(List<Object> args, boolean allowCustomPayload) {
        List<Object> command = validatedCommand(args);
        String name = commandName(command.get(0)).toUpperCase(Locale.ROOT);
        if (!dedicatedSession && DEDICATED_SESSION_COMMANDS.contains(name)) {
            throw new InvalidCommandException(
                    name + " requires transaction() or pubsubSession() on native TCP/TLS");
        }
        long laneId = laneFor(command);
        FlowManyCommandEncoder.Prepared flowMany =
                FlowManyCommandEncoder.tryPrepare(name, command.subList(1, command.size()));
        if (flowMany != null) {
            if (allowCustomPayload) {
                NativeFlowManyCodec.Encoded compact = NativeFlowManyCodec.tryEncode(flowMany);
                if (compact != null) {
                    return new PreparedCommand(
                            flowMany.opcode(),
                            laneId,
                            compact.payload(),
                            NativeProtocol.FLAG_CUSTOM_PAYLOAD);
                }
            }
            return new PreparedCommand(flowMany.opcode(), laneId, flowMany.payload(), 0);
        }
        FlowCommandEncoder.Prepared structured =
                FlowCommandEncoder.prepare(name, command.subList(1, command.size()));
        if (structured != null) {
            return new PreparedCommand(structured.opcode(), laneId, structured.payload(), 0);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("command", name);
        payload.put("args", new ArrayList<>(command.subList(1, command.size())));
        return new PreparedCommand(NativeProtocol.OP_COMMAND_EXEC, laneId, payload, 0);
    }

    private static List<Object> requirePipelineResults(Object value, int expected) {
        if (value instanceof NativeCompactResponseCodec.PipelineResults compact) {
            if (compact.values().size() != expected) {
                throw new NativeProtocolException(
                        "native PIPELINE returned an invalid number of results");
            }
            NativeCompactResponseCodec.PipelineFailure failure = compact.firstFailure();
            if (failure != null) {
                throw new NativeServerException(pipelineStatus(failure.status()), failure.value());
            }
            return compact.values();
        }
        if (!(value instanceof List<?> results) || results.size() != expected) {
            throw new NativeProtocolException(
                    "native PIPELINE returned an invalid number of results");
        }
        List<Object> values = new ArrayList<>(expected);
        for (int index = 0; index < results.size(); index++) {
            Object item = results.get(index);
            if (!(item instanceof List<?> pair) || pair.size() != 2) {
                throw new NativeProtocolException(
                        "native PIPELINE returned an invalid result at index " + index);
            }
            String status = responseToken(pair.get(0));
            if (!"ok".equals(status)) {
                throw new NativeServerException(pipelineStatus(status), pair.get(1));
            }
            values.add(pair.get(1));
        }
        return java.util.Collections.unmodifiableList(values);
    }

    private static List<Object> requireFlowManyResults(Object value, int expected) {
        if ("ok".equals(responseToken(value))) {
            return java.util.Collections.nCopies(expected, value);
        }
        if (!(value instanceof List<?> results) || results.size() != expected) {
            throw new NativeProtocolException(
                    "native FLOW.CREATE_MANY returned an invalid number of results");
        }
        for (Object item : results) {
            if (item instanceof List<?> pair && pair.size() == 2) {
                String status = responseToken(pair.get(0));
                if (!"ok".equals(status)) {
                    throw new NativeServerException(pipelineStatus(status), pair.get(1));
                }
            }
        }
        return java.util.Collections.unmodifiableList(new ArrayList<>(results));
    }

    private static int pipelineStatus(String status) {
        return switch (status) {
            case "auth" -> NativeProtocol.STATUS_AUTH;
            case "noperm" -> NativeProtocol.STATUS_NOPERM;
            case "busy" -> NativeProtocol.STATUS_BUSY;
            case "reroute" -> NativeProtocol.STATUS_REROUTE;
            case "bad_request" -> NativeProtocol.STATUS_BAD_REQUEST;
            default -> NativeProtocol.STATUS_ERROR;
        };
    }

    private static String responseToken(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        }
        return String.valueOf(value).toLowerCase(Locale.ROOT);
    }

    private CompletableFuture<Object> requestWithRetryAsync(
            int opcode, long laneId, Object payload) {
        byte[] body;
        try {
            body = NativeValueCodec.encode(payload, MAX_REQUEST_BYTES);
            validateUnauthenticatedSize(body.length);
        } catch (NativeProtocolException error) {
            return AsyncFutures.failed(error.asNotSent());
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("failed to encode native request", error));
        }
        return requestWithRetryAsync(opcode, laneId, body, 0);
    }

    private CompletableFuture<Object> requestWithRetryAsync(
            int opcode, long laneId, byte[] body, int flags) {
        return requestWithRetryAsync(opcode, laneId, body, flags, deadlineAfter(requestTimeout));
    }

    private CompletableFuture<Object> requestWithRetryAsync(
            int opcode, long laneId, byte[] body, int flags, long deadlineNanos) {
        try {
            validateRequestBody(body);
        } catch (NativeProtocolException error) {
            return AsyncFutures.failed(error.asNotSent());
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("invalid native request body", error));
        }
        CompletableFuture<Object> result = new CompletableFuture<>();
        RetryOperation operation = new RetryOperation(result);
        operation
                .result()
                .whenComplete(
                        (ignored, failure) -> {
                            operation.cancelScheduled();
                            retryOperations.remove(operation);
                        });
        requestAttempt(opcode, laneId, body, flags, 0, deadlineNanos, operation);
        return result;
    }

    private void requestAttempt(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int retries,
            long deadlineNanos,
            RetryOperation operation) {
        CompletableFuture<Object> result = operation.result();
        if (result.isDone()) {
            return;
        }
        if (deadlineExpired(deadlineNanos)) {
            result.completeExceptionally(requestTimeoutBeforeSend());
            return;
        }
        CompletableFuture<NativeResponseCodec.Response> attempt =
                requestEncodedAsync(opcode, laneId, body, flags, deadlineNanos, false);
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        attempt.cancel(false);
                    }
                });
        attempt.whenComplete(
                (response, failure) -> {
                    if (failure != null) {
                        Throwable unwrapped = AsyncFutures.unwrap(failure);
                        RuntimeException error =
                                unwrapped instanceof RuntimeException runtime
                                        ? runtime
                                        : new NativeProtocolException(
                                                "native request failed", unwrapped);
                        if (isGoAwayFailure(error)
                                && !dedicatedSession
                                && !deadlineExpired(deadlineNanos)) {
                            requestAttempt(
                                    opcode, laneId, body, flags, retries, deadlineNanos, operation);
                        } else {
                            result.completeExceptionally(error);
                        }
                        return;
                    }
                    completeAttempt(
                            opcode,
                            laneId,
                            body,
                            flags,
                            retries,
                            deadlineNanos,
                            operation,
                            response);
                });
    }

    private void completeAttempt(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int retries,
            long deadlineNanos,
            RetryOperation operation,
            NativeResponseCodec.Response response) {
        CompletableFuture<Object> result = operation.result();
        try {
            Object value = NativeResponseCodec.requireOk(response);
            if (deadlineExpired(deadlineNanos)) {
                result.completeExceptionally(requestTimeout());
                return;
            }
            result.complete(value);
        } catch (NativeServerException error) {
            if (deadlineExpired(deadlineNanos)) {
                result.completeExceptionally(requestTimeout(error));
                return;
            }
            if (!NativeRetryPolicy.shouldRetry(error, retries)) {
                result.completeExceptionally(error);
                return;
            }
            long remainingNanos = remainingNanos(deadlineNanos);
            if (remainingNanos <= 0) {
                result.completeExceptionally(requestTimeout());
                return;
            }
            long retryDelayNanos =
                    boundedRetryDelayNanos(NativeRetryPolicy.retryAfterMs(error), remainingNanos);
            scheduleRetry(
                    opcode,
                    laneId,
                    body,
                    flags,
                    retries,
                    deadlineNanos,
                    retryDelayNanos,
                    operation);
        } catch (RuntimeException error) {
            result.completeExceptionally(error);
        }
    }

    private void scheduleRetry(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int retries,
            long deadlineNanos,
            long retryDelayNanos,
            RetryOperation operation) {
        CompletableFuture<Object> result = operation.result();
        synchronized (retryOperations) {
            if (result.isDone()) {
                return;
            }
            RuntimeException termination = terminationFailure.get();
            if (closed.get() || termination != null) {
                operation.cancel(
                        termination == null
                                ? new NativeProtocolException(
                                        "native executor closed with requests in flight; outcome is unknown")
                                : termination);
                return;
            }
            retryOperations.add(operation);
            RetryTask task =
                    operation.addTask(
                            () ->
                                    requestAttempt(
                                            opcode,
                                            laneId,
                                            body,
                                            flags,
                                            retries + 1,
                                            deadlineNanos,
                                            operation));
            if (task == null) {
                retryOperations.remove(operation);
                return;
            }
            try {
                ScheduledFuture<?> scheduled =
                        retryExecutor.schedule(task, retryDelayNanos, TimeUnit.NANOSECONDS);
                task.attach(scheduled);
                if (result.isDone()) {
                    retryOperations.remove(operation);
                }
            } catch (RejectedExecutionException rejected) {
                retryOperations.remove(operation);
                RuntimeException currentTermination = terminationFailure.get();
                operation.cancel(currentTermination == null ? rejected : currentTermination);
            }
        }
    }

    @Override
    public Object flowQuery(String query, Map<String, ?> params) {
        return AsyncFutures.await(
                flowQueryAsync(query, params),
                error ->
                        new NativeProtocolException(
                                "native Flow query was interrupted after sending; outcome is unknown",
                                error));
    }

    @Override
    public CompletableFuture<Object> flowQueryAsync(String query, Map<String, ?> params) {
        try {
            if (query == null || query.isBlank()) {
                throw new IllegalArgumentException("Flow query must not be blank");
            }
            Objects.requireNonNull(params, "query params");
            Map<String, Object> typedParams = new LinkedHashMap<>();
            params.forEach(
                    (name, value) -> {
                        if (name == null || name.isBlank()) {
                            throw new IllegalArgumentException(
                                    "Flow query parameter names must not be blank");
                        }
                        typedParams.put(
                                name, Objects.requireNonNull(value, "query parameter value"));
                    });
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("version", "FQL1");
            payload.put("query", query);
            payload.put("params", typedParams);
            Object route = typedParams.getOrDefault("partition", query);
            long laneId = laneForRoute(route);
            return AsyncFutures.map(
                    requestAsync(NativeProtocol.OP_FLOW_QUERY, laneId, payload),
                    NativeResponseCodec::requireOk);
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("failed to prepare native Flow query", error));
        }
    }

    private NativeResponseCodec.Response request(int opcode, long laneId, Object payload) {
        return AsyncFutures.await(
                requestAsync(opcode, laneId, payload),
                error ->
                        new NativeProtocolException(
                                "native request was interrupted after sending; outcome is unknown",
                                error));
    }

    private CompletableFuture<NativeResponseCodec.Response> requestAsync(
            int opcode, long laneId, Object payload) {
        byte[] body;
        try {
            body = NativeValueCodec.encode(payload, MAX_REQUEST_BYTES);
            validateUnauthenticatedSize(body.length);
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("failed to encode native request", error));
        }
        return requestEncodedAsync(opcode, laneId, body, 0, deadlineAfter(requestTimeout), false);
    }

    private CompletableFuture<NativeResponseCodec.Response> requestEncodedAsync(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            long deadlineNanos,
            boolean priorAttemptSent) {
        if (closed.get()) {
            return AsyncFutures.failed(
                    NativeProtocolException.notSent("native connection is closed"));
        }
        if (draining.get()) {
            return requestOnReplacementAsync(
                    opcode, laneId, body, flags, deadlineNanos, priorAttemptSent);
        }
        try {
            validateRequestBody(body);
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("invalid native request body", error));
        }
        if (deadlineExpired(deadlineNanos)) {
            return AsyncFutures.failed(
                    priorAttemptSent ? requestTimeout() : requestTimeoutBeforeSend());
        }
        if (!pendingSlots.tryAcquire()) {
            return AsyncFutures.failed(
                    NativeProtocolException.notSent("native pending request limit exceeded"));
        }
        if (closed.get()) {
            pendingSlots.release();
            return AsyncFutures.failed(
                    NativeProtocolException.notSent("native connection is closed"));
        }
        long requestId;
        try {
            requestId = nextRequestId();
        } catch (RuntimeException error) {
            pendingSlots.release();
            return AsyncFutures.failed(notSent("failed to allocate native request id", error));
        }
        NativeFrame.Identity identity = new NativeFrame.Identity(laneId, opcode, requestId);
        CompletableFuture<NativeResponseCodec.Response> wireResponse = new CompletableFuture<>();
        CompletableFuture<NativeResponseCodec.Response> result = new CompletableFuture<>();
        PendingRequest request =
                new PendingRequest(
                        identity,
                        wireResponse,
                        deadlineNanos,
                        new AtomicBoolean(),
                        new AtomicBoolean(),
                        priorAttemptSent,
                        socket,
                        output);
        pending.put(requestId, request);

        wireResponse
                .orTimeout(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS)
                .whenComplete(
                        (response, failure) -> {
                            removePending(requestId, request);
                            if (wireResponse.isCancelled()) {
                                abortIncompleteWrite(request);
                                return;
                            }
                            if (failure instanceof TimeoutException
                                    || deadlineExpired(deadlineNanos)) {
                                NativeProtocolException timeout = timeoutFailure(request, failure);
                                result.completeExceptionally(timeout);
                                abortIncompleteWrite(request);
                            } else if (failure != null) {
                                result.completeExceptionally(AsyncFutures.unwrap(failure));
                            } else {
                                result.complete(response);
                            }
                        });
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        removePending(requestId, request);
                        wireResponse.cancel(false);
                    }
                });

        if (closed.get() && removePending(requestId, request)) {
            wireResponse.completeExceptionally(
                    NativeProtocolException.notSent("native connection is closed"));
            return result;
        }

        try {
            writeExecutor.execute(
                    () -> writeRequest(laneId, opcode, requestId, flags, body, request));
        } catch (RejectedExecutionException error) {
            if (removePending(requestId, request)) {
                wireResponse.completeExceptionally(
                        NativeProtocolException.notSent("native connection is closed", error));
            }
        }
        return result;
    }

    private CompletableFuture<NativeResponseCodec.Response> requestOnReplacementAsync(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            long deadlineNanos,
            boolean priorAttemptSent) {
        CompletableFuture<NativeResponseCodec.Response> result = new CompletableFuture<>();
        long remaining = remainingNanos(deadlineNanos);
        if (remaining <= 0) {
            result.completeExceptionally(
                    priorAttemptSent ? requestTimeout() : requestTimeoutBeforeSend());
            return result;
        }
        CompletableFuture<NativeExecutor> waitForReplacement = replacementAsync().copy();
        waitForReplacement.orTimeout(remaining, TimeUnit.NANOSECONDS);
        AtomicReference<CompletableFuture<NativeResponseCodec.Response>> delegated =
                new AtomicReference<>();
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        CompletableFuture<NativeResponseCodec.Response> current = delegated.get();
                        if (current != null) {
                            current.cancel(false);
                        }
                        waitForReplacement.cancel(false);
                    }
                });
        waitForReplacement.whenComplete(
                (next, failure) -> {
                    if (failure != null) {
                        Throwable error = AsyncFutures.unwrap(failure);
                        if (error instanceof TimeoutException) {
                            result.completeExceptionally(
                                    priorAttemptSent
                                            ? requestTimeout(error)
                                            : requestTimeoutBeforeSend(error));
                        } else if (priorAttemptSent) {
                            result.completeExceptionally(uncertainOutcome(error));
                        } else {
                            result.completeExceptionally(
                                    NativeProtocolException.notSent(
                                            "failed to reconnect native connection", error));
                        }
                        return;
                    }
                    CompletableFuture<NativeResponseCodec.Response> current =
                            next.requestEncodedAsync(
                                    opcode, laneId, body, flags, deadlineNanos, priorAttemptSent);
                    delegated.set(current);
                    if (result.isCancelled()) {
                        current.cancel(false);
                        return;
                    }
                    current.whenComplete(
                            (response, currentFailure) -> {
                                if (currentFailure != null) {
                                    result.completeExceptionally(
                                            AsyncFutures.unwrap(currentFailure));
                                } else {
                                    result.complete(response);
                                }
                            });
                });
        return result;
    }

    private CompletableFuture<NativeExecutor> replacementAsync() {
        CompletableFuture<NativeExecutor> current = replacement.get();
        if (current != null) {
            return current;
        }
        CompletableFuture<NativeExecutor> created = new CompletableFuture<>();
        if (!replacement.compareAndSet(null, created)) {
            return replacement.get();
        }
        try {
            reconnectExecutor.execute(
                    () -> {
                        NativeExecutor next = null;
                        try {
                            next =
                                    new NativeExecutor(
                                            endpoint,
                                            transportOptions,
                                            dedicatedSession,
                                            nanoTime,
                                            requestTimeout);
                            next.initialize();
                            if (closed.get()) {
                                next.close();
                                replacement.compareAndSet(created, null);
                                created.completeExceptionally(
                                        NativeProtocolException.notSent(
                                                "native executor is closed"));
                            } else {
                                negotiatedCapabilities.set(next.negotiatedCapabilities.get());
                                created.complete(next);
                            }
                        } catch (IOException | RuntimeException error) {
                            if (next != null) {
                                next.close();
                            }
                            replacement.compareAndSet(created, null);
                            created.completeExceptionally(error);
                        }
                    });
        } catch (RejectedExecutionException error) {
            replacement.compareAndSet(created, null);
            created.completeExceptionally(error);
        }
        return created;
    }

    private void writeRequest(
            long laneId,
            int opcode,
            long requestId,
            int flags,
            byte[] body,
            PendingRequest request) {
        try {
            synchronized (writeLock) {
                if (!markWriteStarted(request)) {
                    return;
                }
                NativeFrame.writeRequest(request.output(), laneId, opcode, requestId, flags, body);
                request.output().flush();
                synchronized (request) {
                    request.writeComplete().set(true);
                }
            }
        } catch (IOException error) {
            removePending(requestId, request);
            NativeProtocolException uncertain = uncertainOutcome(error);
            request.future().completeExceptionally(uncertain);
            failTransport(uncertain);
        }
    }

    private boolean markWriteStarted(PendingRequest request) {
        RuntimeException failure;
        synchronized (request) {
            if (request.future().isDone()) {
                return false;
            }
            if (closed.get()) {
                failure = NativeProtocolException.notSent("native connection is closed");
            } else if (draining.get()) {
                failure = goAwayFailure();
            } else if (deadlineExpired(request.deadlineNanos())) {
                failure = requestTimeoutBeforeSend();
            } else {
                request.sent().set(true);
                if (request.identity().opcode() != NativeProtocol.OP_HELLO
                        && request.identity().opcode() != NativeProtocol.OP_AUTH) {
                    dataWriteStarted.countDown();
                }
                return true;
            }
        }
        if (removePending(request.identity().requestId(), request)) {
            request.future().completeExceptionally(failure);
        }
        retireIfGoAwayDrained();
        return false;
    }

    private void abortIncompleteWrite(PendingRequest request) {
        boolean incomplete;
        synchronized (request) {
            incomplete = request.sent().get() && !request.writeComplete().get();
        }
        if (incomplete) {
            failTransport(
                    new NativeProtocolException(
                            "native request write was interrupted; connection was closed"));
        }
    }

    private static RuntimeException notSent(String message, RuntimeException failure) {
        if (failure instanceof NativeProtocolException protocol) {
            return protocol.asNotSent();
        }
        if (failure instanceof RequestDeliveryFailure) {
            return failure;
        }
        return NativeProtocolException.notSent(message, failure);
    }

    private boolean removePending(long requestId, PendingRequest request) {
        if (pending.remove(requestId, request)) {
            pendingSlots.release();
            retireIfGoAwayDrained();
            return true;
        }
        return false;
    }

    private PendingRequest removePending(long requestId) {
        PendingRequest request = pending.remove(requestId);
        if (request != null) {
            pendingSlots.release();
            retireIfGoAwayDrained();
        }
        return request;
    }

    private void retireIfGoAwayDrained() {
        if (draining.get() && pending.isEmpty() && transportRetired.compareAndSet(false, true)) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The old GOAWAY transport is no longer eligible for writes.
            }
            writeExecutor.shutdownNow();
        }
    }

    private void retireAfterGoAway(RuntimeException failure) {
        if (transportRetired.compareAndSet(false, true)) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The original protocol or transport failure remains authoritative.
            }
            writeExecutor.shutdownNow();
        }
        pending.forEach(
                (requestId, request) -> {
                    if (removePending(requestId, request)) {
                        request.future().completeExceptionally(deliveryFailure(request, failure));
                    }
                });
    }

    private static RuntimeException deliveryFailure(
            PendingRequest request, RuntimeException failure) {
        synchronized (request) {
            if (request.priorAttemptSent() || request.sent().get()) {
                return failure;
            }
        }
        return NativeProtocolException.notSent(
                "native connection failed before the request was sent", failure);
    }

    private static RuntimeException goAwayFailure() {
        return NativeProtocolException.notSent(GOAWAY_MESSAGE, new GoAwaySignal());
    }

    private static RuntimeException dedicatedGoAwayFailure() {
        return new NativeProtocolException(GOAWAY_MESSAGE, new GoAwaySignal());
    }

    private static boolean isGoAwayFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof GoAwaySignal) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void validateRequestBody(byte[] body) {
        Objects.requireNonNull(body, "native request body");
        if (body.length > MAX_REQUEST_BYTES) {
            throw NativeProtocolException.notSent(
                    "native request exceeds the maximum request size");
        }
        validateUnauthenticatedSize(body.length);
    }

    @Override
    public void close() {
        terminate(
                new NativeProtocolException(
                        "native executor closed with requests in flight; outcome is unknown"));
    }

    @Override
    public Object pollEvent(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        try {
            Object event = events.poll(durationToNanos(timeout), TimeUnit.NANOSECONDS);
            if (event == CLOSED_EVENT) {
                RuntimeException failure = eventFailure.get();
                throw failure == null
                        ? new NativeProtocolException("native event connection is closed")
                        : failure;
            }
            return event;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new NativeProtocolException("native event wait was interrupted", error);
        }
    }

    private void initialize() {
        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("compression", "none");
        hello.put("client_name", "ferricstore-java");
        hello.put("compact_response_codecs", List.of("kv_mget_v1", "ok_list_v1", "pipeline_v1"));
        Object helloValue =
                NativeResponseCodec.requireOk(request(NativeProtocol.OP_HELLO, 0, hello));
        NegotiatedCapabilities capabilities = NativeHelloContract.parse(helloValue);
        int effectiveLimit =
                Math.min(
                        NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES, capabilities.maxResponseBytes());
        assembler.reconfigure(effectiveLimit);
        maxFrameBytes.set(effectiveLimit);
        negotiatedCapabilities.set(capabilities);

        if (capabilities.authRequired() && endpoint.password() == null) {
            throw new NativeProtocolException(
                    "FerricStore requires authentication; provide credentials in the ferric URI");
        }
        if (endpoint.password() != null) {
            Map<String, Object> auth =
                    Map.of("username", endpoint.username(), "password", endpoint.password());
            NativeResponseCodec.requireOk(request(NativeProtocol.OP_AUTH, 0, auth));
        }
        authenticated.set(!capabilities.authRequired() || endpoint.password() != null);
    }

    private void readLoop(InputStream connectionInput) {
        try {
            while (!closed.get() && !transportRetired.get()) {
                NativeFrame frame = NativeFrame.readResponse(connectionInput, maxFrameBytes::get);
                if (transportRetired.get()) {
                    return;
                }
                NativeResponseAssembler.Assembled assembled =
                        assembler.add(frame.identity(), frame.flags(), frame.body());
                if (assembled == null) {
                    continue;
                }
                if (assembled.identity().requestId() == 0) {
                    Object event =
                            NativeResponseCodec.requireOk(
                                    NativeResponseCodec.decode(assembled.body()));
                    if (!events.offer(event)) {
                        NativeProtocolException overflow = eventBufferOverflow();
                        failTransport(overflow);
                        return;
                    }
                    if (assembled.identity().opcode() == NativeProtocol.OP_GOAWAY) {
                        handleGoAway();
                    }
                    continue;
                }
                PendingRequest request = removePending(assembled.identity().requestId());
                if (request == null) {
                    continue;
                }
                if (!request.identity().equals(assembled.identity())) {
                    NativeProtocolException mismatch = identityMismatch(request, assembled);
                    request.future().completeExceptionally(mismatch);
                    failTransport(mismatch);
                    return;
                }
                try {
                    NativeResponseCodec.Response response;
                    if ((assembled.flags() & NativeProtocol.FLAG_CUSTOM_PAYLOAD) != 0) {
                        String codec =
                                negotiatedCapabilities
                                        .get()
                                        .compactResponseCodecs()
                                        .get(assembled.identity().opcode());
                        if (codec == null) {
                            throw unsupportedCustomPayload(assembled);
                        }
                        response = NativeCompactResponseCodec.decode(codec, assembled.body());
                    } else {
                        response = NativeResponseCodec.decode(assembled.body());
                    }
                    if (deadlineExpired(request.deadlineNanos())) {
                        request.future().completeExceptionally(requestTimeout());
                    } else {
                        request.future().complete(response);
                    }
                } catch (RuntimeException error) {
                    RuntimeException failure =
                            deadlineExpired(request.deadlineNanos())
                                    ? requestTimeout(error)
                                    : error;
                    request.future().completeExceptionally(failure);
                    failTransport(failure);
                    return;
                }
            }
        } catch (IOException error) {
            if (!closed.get()) {
                failTransport(uncertainOutcome(error));
            }
        } catch (RuntimeException error) {
            if (!closed.get()) {
                failTransport(error);
            }
        }
    }

    private void handleGoAway() {
        if (draining.compareAndSet(false, true)) {
            synchronized (writeLock) {
                if (dedicatedSession) {
                    terminate(dedicatedGoAwayFailure());
                } else {
                    retireIfGoAwayDrained();
                }
            }
        }
    }

    private void failTransport(RuntimeException failure) {
        if (draining.get()) {
            retireAfterGoAway(failure);
        } else {
            terminate(failure);
        }
    }

    private static NativeProtocolException eventBufferOverflow() {
        return new NativeProtocolException(
                "native event buffer exceeded " + MAX_BUFFERED_EVENTS + " events");
    }

    private void validateUnauthenticatedSize(int bodyBytes) {
        NegotiatedCapabilities capabilities = negotiatedCapabilities.get();
        if (capabilities != null
                && capabilities.authRequired()
                && !authenticated.get()
                && bodyBytes > NativeProtocol.UNAUTHENTICATED_MAX_FRAME_BYTES) {
            throw NativeProtocolException.notSent(
                    "authenticate before submitting requests larger than the unauthenticated 64 KiB limit");
        }
    }

    private void terminate(RuntimeException failure) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        terminationFailure.set(failure);
        cancelSessionOpenings();
        try {
            socket.close();
        } catch (IOException ignored) {
            // The original protocol or transport failure remains authoritative.
        }
        writeExecutor.shutdownNow();
        reconnectExecutor.shutdownNow();
        synchronized (retryOperations) {
            retryExecutor.shutdownNow();
            retryOperations.forEach(operation -> operation.cancel(failure));
            retryOperations.clear();
        }
        CompletableFuture<NativeExecutor> pendingReplacement = replacement.get();
        if (pendingReplacement != null) {
            pendingReplacement.whenComplete(
                    (next, replacementFailure) -> {
                        if (next != null) {
                            next.close();
                        }
                    });
        }
        assembler.clear();
        pending.forEach(
                (requestId, request) -> {
                    if (removePending(requestId, request)) {
                        request.future().completeExceptionally(deliveryFailure(request, failure));
                    }
                });
        eventFailure.compareAndSet(null, failure);
        if (!events.offer(CLOSED_EVENT)) {
            events.clear();
            if (!events.offer(CLOSED_EVENT)) {
                eventFailure.compareAndSet(
                        null,
                        new NativeProtocolException(
                                "native event stream closed without a terminal marker"));
            }
        }
    }

    private static NativeProtocolException identityMismatch(
            PendingRequest request, NativeResponseAssembler.Assembled assembled) {
        return new NativeProtocolException(
                "native response identity mismatch: expected "
                        + request.identity()
                        + ", got "
                        + assembled.identity());
    }

    private static NativeProtocolException unsupportedCustomPayload(
            NativeResponseAssembler.Assembled assembled) {
        return new NativeProtocolException(
                "server sent an unnegotiated custom response payload for opcode 0x"
                        + Integer.toHexString(assembled.identity().opcode()));
    }

    private long nextRequestId() {
        return requestIds.updateAndGet(current -> current == Long.MAX_VALUE ? 1 : current + 1);
    }

    private static List<Object> validatedCommand(List<Object> args) {
        Objects.requireNonNull(args, "command args");
        if (args.isEmpty()) {
            throw new IllegalArgumentException("FerricStore command must not be empty");
        }
        for (int index = 0; index < args.size(); index++) {
            Object value = args.get(index);
            if (value == null) {
                throw new IllegalArgumentException(
                        "FerricStore command argument cannot be null at index " + index);
            }
        }
        commandName(args.get(0));
        return args;
    }

    private static String commandName(Object value) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        if (value instanceof byte[] bytes) {
            try {
                String decoded =
                        StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(bytes))
                                .toString();
                if (!decoded.isBlank()) {
                    return decoded;
                }
            } catch (CharacterCodingException error) {
                throw new IllegalArgumentException("command name must be valid UTF-8", error);
            }
        }
        throw new IllegalArgumentException("command name must be a non-blank string");
    }

    private static long laneFor(List<Object> command) {
        Object route = NativeRouting.routeKey(command);
        return laneForRoute(route);
    }

    private static long laneForRoute(Object route) {
        int hash = route instanceof byte[] bytes ? Arrays.hashCode(bytes) : route.hashCode();
        return 1L + Math.floorMod(hash, DATA_LANES);
    }

    private static long durationToNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static Duration requirePositiveTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "request timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("request timeout must be positive");
        }
        return timeout;
    }

    static long boundedRetryDelayNanos(long delayMs, long remainingNanos) {
        return Math.min(TimeUnit.MILLISECONDS.toNanos(delayMs), remainingNanos);
    }

    private long deadlineAfter(Duration timeout) {
        long timeoutNanos = durationToNanos(timeout);
        long now = nanoTime.getAsLong();
        return now + timeoutNanos;
    }

    private long remainingNanos(long deadlineNanos) {
        long remaining = deadlineNanos - nanoTime.getAsLong();
        return remaining > 0 ? remaining : 0;
    }

    private boolean deadlineExpired(long deadlineNanos) {
        return remainingNanos(deadlineNanos) <= 0;
    }

    private static NativeProtocolException requestTimeout() {
        return requestTimeout(new TimeoutException("native request deadline exceeded"));
    }

    private static NativeProtocolException requestTimeoutBeforeSend() {
        return requestTimeoutBeforeSend(
                new TimeoutException("native request deadline exceeded before sending"));
    }

    private static NativeProtocolException requestTimeoutBeforeSend(Throwable cause) {
        return NativeProtocolException.notSent("native request timed out before sending", cause);
    }

    private static NativeProtocolException requestTimeout(Throwable cause) {
        return new NativeProtocolException(
                "native request timed out after sending; outcome is unknown", cause);
    }

    private static NativeProtocolException timeoutFailure(PendingRequest request, Throwable cause) {
        synchronized (request) {
            return request.priorAttemptSent() || request.sent().get()
                    ? requestTimeout(cause)
                    : requestTimeoutBeforeSend(cause);
        }
    }

    private static NativeProtocolException uncertainOutcome(Throwable cause) {
        return new NativeProtocolException(
                "native connection failed after a request was sent; outcome is unknown", cause);
    }

    private static Thread writerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "ferricstore-native-writer");
        thread.setDaemon(true);
        return thread;
    }

    private static Thread reconnectThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "ferricstore-native-reconnect");
        thread.setDaemon(true);
        return thread;
    }

    private static Thread retryThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "ferricstore-native-retry");
        thread.setDaemon(true);
        return thread;
    }

    private static ScheduledThreadPoolExecutor retryExecutor() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1, NativeExecutor::retryThread);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private enum PipelineResponseType {
        PIPELINE,
        FLOW_MANY
    }

    private record PreparedPipelineBatch(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int expected,
            PipelineResponseType responseType) {}

    private record PreparedCommand(int opcode, long laneId, Object payload, int flags) {}

    private static Socket connectSocket(
            NativeEndpoint endpoint, SSLContext sslContext, SessionOpening opening)
            throws IOException {
        return connectSocket(
                endpoint,
                sslContext,
                opening,
                new NativeSocketFactory() {
                    @Override
                    public Socket openRaw() {
                        return new Socket();
                    }

                    @Override
                    public Socket createTls(Socket raw, NativeEndpoint target, SSLContext context)
                            throws IOException {
                        return context.getSocketFactory()
                                .createSocket(raw, target.host(), target.port(), true);
                    }
                });
    }

    static Socket connectSocketForTesting(
            NativeEndpoint endpoint, SSLContext sslContext, NativeSocketFactory factory)
            throws IOException {
        return connectSocket(
                endpoint, sslContext, null, Objects.requireNonNull(factory, "factory"));
    }

    private static Socket connectSocket(
            NativeEndpoint endpoint,
            SSLContext sslContext,
            SessionOpening opening,
            NativeSocketFactory factory)
            throws IOException {
        Socket raw = null;
        Socket layered = null;
        try {
            raw = factory.openRaw();
            if (opening != null) {
                opening.attachSocket(raw);
            }
            raw.setTcpNoDelay(true);
            raw.connect(
                    new InetSocketAddress(endpoint.host(), endpoint.port()),
                    Math.toIntExact(CONNECT_TIMEOUT.toMillis()));
            if (!endpoint.tls()) {
                return raw;
            }

            SSLContext context;
            try {
                context = sslContext == null ? SSLContext.getDefault() : sslContext;
            } catch (java.security.NoSuchAlgorithmException error) {
                throw new IOException("default TLS context is unavailable", error);
            }
            layered = factory.createTls(raw, endpoint, context);
            if (!(layered instanceof SSLSocket tls)) {
                throw new IOException("TLS socket factory did not create an SSLSocket");
            }
            if (opening != null) {
                opening.attachSocket(layered);
            }
            SSLParameters parameters = tls.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            tls.setSSLParameters(parameters);
            tls.setSoTimeout(Math.toIntExact(REQUEST_TIMEOUT.toMillis()));
            tls.startHandshake();
            tls.setSoTimeout(0);
            return tls;
        } catch (IOException | RuntimeException error) {
            closeSocket(layered, raw, error);
            throw error;
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static void closeSocket(Socket layered, Socket raw, Throwable failure) {
        // Resource aliasing is based on object identity, not socket value equality.
        if (layered != null) {
            try {
                layered.close();
            } catch (IOException closeError) {
                if (closeError != failure) {
                    failure.addSuppressed(closeError);
                }
            }
        }
        if (raw != null && raw != layered) {
            try {
                raw.close();
            } catch (IOException closeError) {
                if (closeError != failure) {
                    failure.addSuppressed(closeError);
                }
            }
        }
    }

    interface NativeSocketFactory {
        Socket openRaw() throws IOException;

        Socket createTls(Socket raw, NativeEndpoint endpoint, SSLContext context)
                throws IOException;
    }

    private record PendingRequest(
            NativeFrame.Identity identity,
            CompletableFuture<NativeResponseCodec.Response> future,
            long deadlineNanos,
            AtomicBoolean sent,
            AtomicBoolean writeComplete,
            boolean priorAttemptSent,
            Socket connection,
            OutputStream output) {}

    private static final class RetryOperation {
        private final CompletableFuture<Object> result;
        private final Set<RetryTask> scheduled = new HashSet<>();

        private RetryOperation(CompletableFuture<Object> result) {
            this.result = result;
        }

        private CompletableFuture<Object> result() {
            return result;
        }

        private RetryTask addTask(Runnable action) {
            synchronized (this) {
                if (result.isDone()) {
                    return null;
                }
                RetryTask task = new RetryTask(this, action);
                scheduled.add(task);
                return task;
            }
        }

        private boolean start(RetryTask task) {
            synchronized (this) {
                return !result.isDone() && scheduled.remove(task);
            }
        }

        private void remove(RetryTask task) {
            synchronized (this) {
                scheduled.remove(task);
            }
        }

        private void cancelScheduled() {
            List<RetryTask> current;
            synchronized (this) {
                current = new ArrayList<>(scheduled);
                scheduled.clear();
            }
            current.forEach(RetryTask::cancel);
        }

        private void cancel(RuntimeException failure) {
            result.completeExceptionally(failure);
            cancelScheduled();
        }
    }

    private static final class RetryTask implements Runnable {
        private final RetryOperation operation;
        private final Runnable action;
        private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>();
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        private RetryTask(RetryOperation operation, Runnable action) {
            this.operation = operation;
            this.action = action;
        }

        @Override
        public void run() {
            if (!started.compareAndSet(false, true) || !operation.start(this)) {
                return;
            }
            action.run();
        }

        private void attach(ScheduledFuture<?> current) {
            future.set(current);
            if (cancelled.get() || started.get() || operation.result().isDone()) {
                current.cancel(false);
                operation.remove(this);
            }
        }

        private void cancel() {
            cancelled.set(true);
            ScheduledFuture<?> current = future.get();
            if (current != null) {
                current.cancel(false);
            }
            operation.remove(this);
        }
    }

    // Cancellation may race with attachment; cleanup is eventual rather than a blocking barrier.
    private static final class SessionOpening {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicReference<Socket> socket = new AtomicReference<>();
        private final AtomicReference<NativeExecutor> executor = new AtomicReference<>();

        private void attachSocket(Socket current) {
            socket.set(current);
            if (cancelled.get()) {
                closeSocket(current);
            }
        }

        private void attachExecutor(NativeExecutor current) {
            executor.set(current);
            if (cancelled.get()) {
                current.close();
            }
        }

        private boolean cancelled() {
            return cancelled.get();
        }

        private void cancel() {
            if (!cancelled.compareAndSet(false, true)) {
                return;
            }
            closeSocket(socket.get());
            NativeExecutor current = executor.get();
            if (current != null) {
                current.close();
            }
        }

        private static void closeSocket(Socket current) {
            if (current == null) {
                return;
            }
            try {
                current.close();
            } catch (IOException ignored) {
                // Session opening is already being cancelled.
            }
        }
    }

    private static final class GoAwaySignal extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
