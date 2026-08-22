package com.ferricstore;

import java.io.EOFException;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/** Multiplexed TCP/TLS executor for FerricStore's native protocol v1. */
public final class NativeExecutor implements CommandExecutor, AutoCloseable {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int DATA_LANES = 32;
    private static final int MAX_REQUEST_BYTES = 64 * 1024 * 1024;

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
    private final Thread readerThread;

    private volatile int maxFrameBytes = NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES;
    private volatile NegotiatedCapabilities negotiatedCapabilities;
    private volatile boolean authenticated;

    private NativeExecutor(NativeEndpoint endpoint, SSLContext sslContext) throws IOException {
        this.endpoint = endpoint;
        this.socket = connectSocket(endpoint, sslContext);
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
        this.readerThread =
                Thread.ofPlatform()
                        .daemon()
                        .name("ferricstore-native-reader")
                        .start(this::readLoop);
    }

    public static NativeExecutor connect(String uri) {
        return connect(uri, null);
    }

    /** Connects with an optional caller-provided TLS context for {@code ferrics://} URLs. */
    public static NativeExecutor connect(String uri, SSLContext sslContext) {
        NativeEndpoint endpoint = NativeEndpoint.parse(uri);
        NativeExecutor executor = null;
        try {
            executor = new NativeExecutor(endpoint, sslContext);
            executor.initialize();
            return executor;
        } catch (IOException error) {
            if (executor != null) {
                executor.close();
            }
            throw new NativeProtocolException("failed to connect to FerricStore native endpoint", error);
        } catch (RuntimeException error) {
            if (executor != null) {
                executor.close();
            }
            throw error;
        }
    }

    public NegotiatedCapabilities negotiatedCapabilities() {
        NegotiatedCapabilities current = negotiatedCapabilities;
        if (current == null) {
            throw new IllegalStateException("FerricStore HELLO negotiation is not complete");
        }
        return current;
    }

    @Override
    public Object execute(List<Object> args) {
        List<Object> command = validatedCommand(args);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("command", commandName(command.getFirst()));
        payload.put("args", new ArrayList<>(command.subList(1, command.size())));
        long laneId = laneFor(command);

        int retries = 0;
        while (true) {
            try {
                NativeResponseCodec.Response response =
                        request(NativeProtocol.OP_COMMAND_EXEC, laneId, payload);
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
    public void close() {
        terminate(
                new NativeProtocolException(
                        "native executor closed with requests in flight; outcome is unknown"));
    }

    private void initialize() {
        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("compression", "none");
        hello.put("client_name", "ferricstore-java");
        Object helloValue =
                NativeResponseCodec.requireOk(request(NativeProtocol.OP_HELLO, 0, hello));
        NegotiatedCapabilities capabilities = NativeHelloContract.parse(helloValue);
        int effectiveLimit =
                Math.min(NativeProtocol.DEFAULT_MAX_RESPONSE_BYTES, capabilities.maxResponseBytes());
        assembler.reconfigure(effectiveLimit);
        maxFrameBytes = effectiveLimit;
        negotiatedCapabilities = capabilities;

        if (capabilities.authRequired() && endpoint.password() == null) {
            throw new NativeProtocolException(
                    "FerricStore requires authentication; provide credentials in the ferric URI");
        }
        if (endpoint.password() != null) {
            Map<String, Object> auth =
                    Map.of("username", endpoint.username(), "password", endpoint.password());
            NativeResponseCodec.requireOk(request(NativeProtocol.OP_AUTH, 0, auth));
        }
        authenticated = !capabilities.authRequired() || endpoint.password() != null;
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
                NativeFrame frame = NativeFrame.readResponse(input, () -> maxFrameBytes);
                NativeResponseAssembler.Assembled assembled =
                        assembler.add(frame.identity(), frame.flags(), frame.body());
                if (assembled == null || assembled.identity().requestId() == 0) {
                    continue;
                }
                PendingRequest request = pending.remove(assembled.identity().requestId());
                if (request == null) {
                    continue;
                }
                if (!request.identity().equals(assembled.identity())) {
                    NativeProtocolException mismatch =
                            new NativeProtocolException(
                                    "native response identity mismatch: expected "
                                            + request.identity()
                                            + ", got "
                                            + assembled.identity());
                    request.future().completeExceptionally(mismatch);
                    terminate(mismatch);
                    return;
                }
                NegotiatedCapabilities capabilities = negotiatedCapabilities;
                if (capabilities != null
                        && capabilities.compactResponseCodecs().containsKey(
                                assembled.identity().opcode())) {
                    NativeProtocolException unsupported =
                            new NativeProtocolException(
                                    "server selected compact response codec "
                                            + capabilities
                                                    .compactResponseCodecs()
                                                    .get(assembled.identity().opcode())
                                            + " for unsupported opcode 0x"
                                            + Integer.toHexString(assembled.identity().opcode()));
                    request.future().completeExceptionally(unsupported);
                    terminate(unsupported);
                    return;
                }
                request.future().complete(NativeResponseCodec.decode(assembled.body()));
            }
        } catch (EOFException error) {
            if (!closed.get()) {
                terminate(uncertainOutcome(error));
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

    private void validateUnauthenticatedSize(int bodyBytes) {
        NegotiatedCapabilities capabilities = negotiatedCapabilities;
        if (capabilities != null
                && capabilities.authRequired()
                && !authenticated
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
        commandName(copy.getFirst());
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
            throw new NativeProtocolException("interrupted during server-directed retry delay", error);
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
        SSLSocket tls =
                (SSLSocket)
                        context.getSocketFactory()
                                .createSocket(raw, endpoint.host(), endpoint.port(), true);
        SSLParameters parameters = tls.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        tls.setSSLParameters(parameters);
        tls.setSoTimeout(Math.toIntExact(REQUEST_TIMEOUT.toMillis()));
        tls.startHandshake();
        tls.setSoTimeout(0);
        return tls;
    }

    private record PendingRequest(
            NativeFrame.Identity identity, CompletableFuture<NativeResponseCodec.Response> future) {}
}
