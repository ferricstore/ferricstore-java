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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    private final AtomicLong requestIds = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<Long, PendingRequest> pending = new ConcurrentHashMap<>();
    private final NativeResponseAssembler assembler =
            new NativeResponseAssembler(
                    NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES,
                    NativeProtocol.DEFAULT_MAX_RESPONSE_CHUNKS);
    private final NativeEndpoint endpoint;
    private final NativeTransportOptions transportOptions;
    private final Semaphore pendingSlots;
    private final boolean dedicatedSession;
    private final Thread readerThread;
    private final BlockingQueue<Object> events = new ArrayBlockingQueue<>(MAX_BUFFERED_EVENTS);
    private final AtomicReference<RuntimeException> eventFailure = new AtomicReference<>();

    private final AtomicInteger maxFrameBytes =
            new AtomicInteger(NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES);
    private final AtomicReference<NegotiatedCapabilities> negotiatedCapabilities =
            new AtomicReference<>();
    private final AtomicBoolean authenticated = new AtomicBoolean();

    private NativeExecutor(
            NativeEndpoint endpoint,
            NativeTransportOptions transportOptions,
            boolean dedicatedSession)
            throws IOException {
        this.endpoint = endpoint;
        this.transportOptions = transportOptions;
        this.pendingSlots = new Semaphore(transportOptions.maxPendingRequests());
        this.dedicatedSession = dedicatedSession;
        Socket connected = connectSocket(endpoint, transportOptions.sslContext());
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
        this.readerThread = new Thread(this::readLoop, "ferricstore-native-reader");
        this.readerThread.setDaemon(true);
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
        Objects.requireNonNull(transportOptions, "native transport options");
        NativeEndpoint endpoint = NativeEndpoint.parse(uri);
        try {
            NativeExecutor executor = new NativeExecutor(endpoint, transportOptions, false);
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

    @Override
    public SessionCommandExecutor openSession() {
        try {
            NativeExecutor session = new NativeExecutor(endpoint, transportOptions, true);
            try {
                session.initialize();
                return session;
            } catch (RuntimeException error) {
                session.close();
                throw error;
            }
        } catch (IOException error) {
            throw new NativeProtocolException(
                    "failed to open dedicated FerricStore native session", error);
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
        return AsyncFutures.map(
                requestWithRetryAsync(batch.opcode(), batch.laneId(), batch.body(), batch.flags()),
                value ->
                        switch (batch.responseType()) {
                            case PIPELINE -> requirePipelineResults(value, batch.expected());
                            case FLOW_MANY -> requireFlowManyResults(value, batch.expected());
                        });
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
        try {
            validateRequestBody(body);
        } catch (NativeProtocolException error) {
            return AsyncFutures.failed(error.asNotSent());
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("invalid native request body", error));
        }
        CompletableFuture<Object> result = new CompletableFuture<>();
        requestAttempt(opcode, laneId, body, flags, 0, result);
        return result;
    }

    private void requestAttempt(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int retries,
            CompletableFuture<Object> result) {
        if (result.isDone()) {
            return;
        }
        CompletableFuture<NativeResponseCodec.Response> attempt =
                requestEncodedAsync(opcode, laneId, body, flags);
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        attempt.cancel(false);
                    }
                });
        attempt.whenComplete(
                (response, failure) -> {
                    if (failure != null) {
                        result.completeExceptionally(AsyncFutures.unwrap(failure));
                        return;
                    }
                    completeAttempt(opcode, laneId, body, flags, retries, result, response);
                });
    }

    private void completeAttempt(
            int opcode,
            long laneId,
            byte[] body,
            int flags,
            int retries,
            CompletableFuture<Object> result,
            NativeResponseCodec.Response response) {
        try {
            result.complete(NativeResponseCodec.requireOk(response));
        } catch (NativeServerException error) {
            if (!NativeRetryPolicy.shouldRetry(error, retries)) {
                result.completeExceptionally(error);
                return;
            }
            long delayMs = NativeRetryPolicy.retryAfterMs(error);
            CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
                    .execute(
                            () -> requestAttempt(opcode, laneId, body, flags, retries + 1, result));
        } catch (RuntimeException error) {
            result.completeExceptionally(error);
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
        return requestEncodedAsync(opcode, laneId, body, 0);
    }

    private CompletableFuture<NativeResponseCodec.Response> requestEncodedAsync(
            int opcode, long laneId, byte[] body, int flags) {
        if (closed.get()) {
            return AsyncFutures.failed(
                    NativeProtocolException.notSent("native connection is closed"));
        }
        try {
            validateRequestBody(body);
        } catch (RuntimeException error) {
            return AsyncFutures.failed(notSent("invalid native request body", error));
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
        PendingRequest request = new PendingRequest(identity, wireResponse);
        pending.put(requestId, request);

        wireResponse
                .orTimeout(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete(
                        (response, failure) -> {
                            removePending(requestId, request);
                            if (failure instanceof TimeoutException) {
                                result.completeExceptionally(
                                        new NativeProtocolException(
                                                "native request timed out after sending; outcome is unknown",
                                                failure));
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
            synchronized (writeLock) {
                NativeFrame.writeRequest(output, laneId, opcode, requestId, flags, body);
                output.flush();
            }
        } catch (IOException error) {
            removePending(requestId, request);
            NativeProtocolException uncertain = uncertainOutcome(error);
            wireResponse.completeExceptionally(uncertain);
            terminate(uncertain);
        }
        return result;
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
            return true;
        }
        return false;
    }

    private PendingRequest removePending(long requestId) {
        PendingRequest request = pending.remove(requestId);
        if (request != null) {
            pendingSlots.release();
        }
        return request;
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

    private void readLoop() {
        try {
            while (!closed.get()) {
                NativeFrame frame = NativeFrame.readResponse(input, maxFrameBytes::get);
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
                        terminate(overflow);
                        return;
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
                    terminate(mismatch);
                    return;
                }
                try {
                    if ((assembled.flags() & NativeProtocol.FLAG_CUSTOM_PAYLOAD) != 0) {
                        String codec =
                                negotiatedCapabilities
                                        .get()
                                        .compactResponseCodecs()
                                        .get(assembled.identity().opcode());
                        if (codec == null) {
                            throw unsupportedCustomPayload(assembled);
                        }
                        request.future()
                                .complete(
                                        NativeCompactResponseCodec.decode(codec, assembled.body()));
                    } else {
                        request.future().complete(NativeResponseCodec.decode(assembled.body()));
                    }
                } catch (RuntimeException error) {
                    request.future().completeExceptionally(error);
                    terminate(error);
                    return;
                }
            }
        } catch (IOException error) {
            if (!closed.get()) {
                terminate(uncertainOutcome(error));
            }
        } catch (RuntimeException error) {
            if (!closed.get()) {
                terminate(error);
            }
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
        if (closed.compareAndSet(false, true)) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The original protocol or transport failure remains authoritative.
            }
        }
        assembler.clear();
        pending.forEach(
                (requestId, request) -> {
                    if (removePending(requestId, request)) {
                        request.future().completeExceptionally(failure);
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

    private static NativeProtocolException uncertainOutcome(Throwable cause) {
        return new NativeProtocolException(
                "native connection failed after a request was sent; outcome is unknown", cause);
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

    private static Socket connectSocket(NativeEndpoint endpoint, SSLContext sslContext)
            throws IOException {
        Socket raw = new Socket();
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
            raw.close();
            throw new IOException("default TLS context is unavailable", error);
        }
        Socket layered;
        try {
            layered =
                    context.getSocketFactory()
                            .createSocket(raw, endpoint.host(), endpoint.port(), true);
        } catch (IOException | RuntimeException error) {
            try {
                raw.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
        if (!(layered instanceof SSLSocket tls)) {
            layered.close();
            throw new IOException("TLS socket factory did not create an SSLSocket");
        }
        SSLParameters parameters = tls.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        tls.setSSLParameters(parameters);
        tls.setSoTimeout(Math.toIntExact(REQUEST_TIMEOUT.toMillis()));
        tls.startHandshake();
        tls.setSoTimeout(0);
        return tls;
    }

    private record PendingRequest(
            NativeFrame.Identity identity,
            CompletableFuture<NativeResponseCodec.Response> future) {}
}
