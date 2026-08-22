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
import java.util.concurrent.ExecutionException;
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
    private final SSLContext sslContext;
    private final boolean dedicatedSession;
    private final Thread readerThread;
    private final BlockingQueue<Object> events = new ArrayBlockingQueue<>(MAX_BUFFERED_EVENTS);
    private final AtomicReference<RuntimeException> eventFailure = new AtomicReference<>();

    private final AtomicInteger maxFrameBytes =
            new AtomicInteger(NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES);
    private final AtomicReference<NegotiatedCapabilities> negotiatedCapabilities =
            new AtomicReference<>();
    private final AtomicBoolean authenticated = new AtomicBoolean();

    private NativeExecutor(NativeEndpoint endpoint, SSLContext sslContext, boolean dedicatedSession)
            throws IOException {
        this.endpoint = endpoint;
        this.sslContext = sslContext;
        this.dedicatedSession = dedicatedSession;
        Socket connected = connectSocket(endpoint, sslContext);
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
        return connect(uri, null);
    }

    /** Connects with an optional caller-provided TLS context for {@code ferrics://} URLs. */
    public static NativeExecutor connect(String uri, SSLContext sslContext) {
        NativeEndpoint endpoint = NativeEndpoint.parse(uri);
        try {
            NativeExecutor executor = new NativeExecutor(endpoint, sslContext, false);
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
            NativeExecutor session = new NativeExecutor(endpoint, sslContext, true);
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
        List<Object> command = validatedCommand(args);
        String name = commandName(command.get(0)).toUpperCase(Locale.ROOT);
        if (!dedicatedSession && DEDICATED_SESSION_COMMANDS.contains(name)) {
            throw new InvalidCommandException(
                    name + " requires transaction() or pubsubSession() on native TCP/TLS");
        }
        long laneId = laneFor(command);
        FlowCommandEncoder.Prepared structured =
                FlowCommandEncoder.prepare(name, command.subList(1, command.size()));
        if (structured != null) {
            return requestWithRetry(structured.opcode(), laneId, structured.payload());
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("command", name);
        payload.put("args", new ArrayList<>(command.subList(1, command.size())));
        return requestWithRetry(NativeProtocol.OP_COMMAND_EXEC, laneId, payload);
    }

    private Object requestWithRetry(int opcode, long laneId, Object payload) {
        int retries = 0;
        while (true) {
            try {
                NativeResponseCodec.Response response = request(opcode, laneId, payload);
                return NativeResponseCodec.requireOk(response);
            } catch (NativeServerException error) {
                if (!NativeRetryPolicy.shouldRetry(error, retries)) {
                    throw error;
                }
                retries++;
                sleepForRetry(NativeRetryPolicy.retryAfterMs(error));
            }
        }
    }

    @Override
    public Object flowQuery(String query, Map<String, ?> params) {
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
                    typedParams.put(name, Objects.requireNonNull(value, "query parameter value"));
                });
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("version", "FQL1");
        payload.put("query", query);
        payload.put("params", typedParams);
        Object route = typedParams.getOrDefault("partition", query);
        long laneId = laneForRoute(route);
        return NativeResponseCodec.requireOk(
                request(NativeProtocol.OP_FLOW_QUERY, laneId, payload));
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

    private NativeResponseCodec.Response request(int opcode, long laneId, Object payload) {
        if (closed.get()) {
            throw new NativeProtocolException("native connection is closed");
        }
        byte[] body = NativeValueCodec.encode(payload, MAX_REQUEST_BYTES);
        validateUnauthenticatedSize(body.length);
        long requestId = nextRequestId();
        NativeFrame.Identity identity = new NativeFrame.Identity(laneId, opcode, requestId);
        PendingRequest request = new PendingRequest(identity, new CompletableFuture<>());
        pending.put(requestId, request);
        if (closed.get() && pending.remove(requestId, request)) {
            throw new NativeProtocolException("native connection is closed");
        }

        try {
            synchronized (writeLock) {
                NativeFrame.writeRequest(output, laneId, opcode, requestId, 0, body);
                output.flush();
            }
        } catch (IOException error) {
            pending.remove(requestId, request);
            NativeProtocolException uncertain = uncertainOutcome(error);
            terminate(uncertain);
            throw uncertain;
        }
        return await(requestId, request);
    }

    @SuppressWarnings("PMD.PreserveStackTrace") // ExecutionException is deliberately unwrapped.
    private NativeResponseCodec.Response await(long requestId, PendingRequest request) {
        try {
            return request.future().get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            pending.remove(requestId, request);
            throw new NativeProtocolException(
                    "native request was interrupted after sending; outcome is unknown", error);
        } catch (TimeoutException error) {
            pending.remove(requestId, request);
            throw new NativeProtocolException(
                    "native request timed out after sending; outcome is unknown", error);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new NativeProtocolException("native request failed", cause);
        }
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
                PendingRequest request = pending.remove(assembled.identity().requestId());
                if (request == null) {
                    continue;
                }
                if (!request.identity().equals(assembled.identity())) {
                    NativeProtocolException mismatch = identityMismatch(request, assembled);
                    request.future().completeExceptionally(mismatch);
                    terminate(mismatch);
                    return;
                }
                if ((assembled.flags() & NativeProtocol.FLAG_CUSTOM_PAYLOAD) != 0) {
                    String codec =
                            negotiatedCapabilities
                                    .get()
                                    .compactResponseCodecs()
                                    .get(assembled.identity().opcode());
                    if (codec == null) {
                        NativeProtocolException unsupported = unsupportedCustomPayload(assembled);
                        request.future().completeExceptionally(unsupported);
                        terminate(unsupported);
                        return;
                    }
                    request.future()
                            .complete(NativeCompactResponseCodec.decode(codec, assembled.body()));
                    continue;
                }
                request.future().complete(NativeResponseCodec.decode(assembled.body()));
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
            throw new NativeProtocolException(
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
                    if (pending.remove(requestId, request)) {
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
        List<Object> copy = new ArrayList<>(args.size());
        for (int index = 0; index < args.size(); index++) {
            Object value = args.get(index);
            if (value == null) {
                throw new IllegalArgumentException(
                        "FerricStore command argument cannot be null at index " + index);
            }
            copy.add(value);
        }
        commandName(copy.get(0));
        return copy;
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

    private static void sleepForRetry(long delayMs) {
        if (delayMs == 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new NativeProtocolException(
                    "interrupted during server-directed retry delay", error);
        }
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
