package com.ferricstore;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.SSLContext;

/** Ordered, binary-safe HTTP/HTTPS executor for FerricStore's stateless command endpoint. */
public final class HttpExecutor implements CommandExecutor, AutoCloseable {
    private static final EncodedBody EMPTY_BODY = new EncodedBody(new byte[0], 0);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> CONNECTION_AFFINE_COMMANDS =
            Set.of(
                    "ASKING",
                    "AUTH",
                    "BACKPRESSURE",
                    "CLIENT",
                    "CLIENT.INFO",
                    "CLIENT.SETNAME",
                    "DISCARD",
                    "EVENT",
                    "EXEC",
                    "FETCH_OR_COMPUTE",
                    "FETCH_OR_COMPUTE_ERROR",
                    "FETCH_OR_COMPUTE_RESULT",
                    "GOAWAY",
                    "HELLO",
                    "MONITOR",
                    "MULTI",
                    "OPTIONS",
                    "PIPELINE",
                    "PSUBSCRIBE",
                    "PSYNC",
                    "PUNSUBSCRIBE",
                    "QUIT",
                    "READONLY",
                    "READWRITE",
                    "REPLCONF",
                    "RESET",
                    "ROUTE",
                    "ROUTE_BATCH",
                    "SANDBOX",
                    "SELECT",
                    "SHARDS",
                    "SSUBSCRIBE",
                    "STARTUP",
                    "SUBSCRIBE",
                    "SUBSCRIBE_EVENTS",
                    "SUNSUBSCRIBE",
                    "SYNC",
                    "UNSUBSCRIBE",
                    "UNSUBSCRIBE_EVENTS",
                    "UNWATCH",
                    "WATCH",
                    "WINDOW_UPDATE");

    private final URI commandEndpoint;
    private final AtomicReference<HttpClient> client;
    private final Map<String, String> headers;
    private final Duration requestTimeout;
    private final int maxRequestBytes;
    private final int maxResponseBytes;
    private final int maxBatchItems;
    private final HttpClient.Redirect redirects;
    private final boolean compact;
    private final AsyncPermitPool requestSlots;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<HttpClient.Version> observedVersion = new AtomicReference<>();

    private HttpExecutor(String endpoint, HttpTransportOptions options) {
        commandEndpoint = commandEndpoint(endpoint, options);
        client = new AtomicReference<>(createClient(options));
        headers = authenticationHeaders(options);
        requestTimeout = options.requestTimeout();
        maxRequestBytes = options.maxRequestBytes();
        maxResponseBytes = options.maxResponseBytes();
        maxBatchItems = options.maxBatchItems();
        redirects = options.redirects();
        compact = options.compact();
        requestSlots =
                new AsyncPermitPool(options.maxConcurrentRequests(), options.maxPendingRequests());
    }

    public static HttpExecutor connect(String endpoint) {
        return connect(endpoint, HttpTransportOptions.defaults());
    }

    public static HttpExecutor connect(String endpoint, HttpTransportOptions options) {
        Objects.requireNonNull(options, "HTTP transport options");
        return new HttpExecutor(endpoint, options);
    }

    /** Returns the protocol version observed on the most recent HTTP response, if any. */
    public HttpClient.Version observedVersion() {
        return observedVersion.get();
    }

    @Override
    public Object execute(List<Object> args) {
        return AsyncFutures.await(
                executeAsync(args),
                error ->
                        transportFailure(
                                "FerricStore HTTP request was interrupted; outcome is unknown",
                                "transport_interrupted",
                                error));
    }

    @Override
    public CompletableFuture<Object> executeAsync(List<Object> args) {
        try {
            return AsyncFutures.map(pipelineAsync(List.of(args)), results -> results.get(0));
        } catch (RuntimeException failure) {
            return AsyncFutures.failed(localFailure(failure));
        }
    }

    @Override
    public List<Object> pipeline(List<List<Object>> commands) {
        return AsyncFutures.await(
                pipelineAsync(commands),
                error ->
                        transportFailure(
                                "FerricStore HTTP request was interrupted; outcome is unknown",
                                "transport_interrupted",
                                error));
    }

    @Override
    public CompletableFuture<List<Object>> pipelineAsync(List<List<Object>> commands) {
        try {
            Objects.requireNonNull(commands, "commands");
            requireOpen();
            if (commands.isEmpty()) {
                return CompletableFuture.completedFuture(List.of());
            }
            if (commands.size() > maxBatchItems) {
                throw new IllegalArgumentException(
                        "HTTP command batch exceeds maxBatchItems=" + maxBatchItems);
            }

            FlowCreatePipeline.Batch flowCreateBatch = FlowCreatePipeline.tryParse(commands);
            EncodedBody requestBody =
                    compact
                            ? encodeMessagePackRequest(commands, flowCreateBatch)
                            : flowCreateBatch == null
                                    ? encodeRequest(commands)
                                    : encodeFlowCreateManyRequest(flowCreateBatch);
            return AsyncFutures.map(
                    sendAsync(requestBody, effectiveRequestTimeout(commands)),
                    response ->
                            flowCreateBatch == null
                                    ? decodePipelineResponse(commands.size(), response)
                                    : decodeFlowCreateManyResponse(
                                            flowCreateBatch.count(), response));
        } catch (RuntimeException failure) {
            return AsyncFutures.failed(localFailure(failure));
        }
    }

    private List<Object> decodeFlowCreateManyResponse(
            int expectedResults, Map<String, Object> response) {
        Object value = decodePipelineResponse(1, response).get(0);
        if ("ok".equals(responseToken(value))) {
            return java.util.Collections.nCopies(expectedResults, value);
        }
        if (!(value instanceof List<?> results) || results.size() != expectedResults) {
            throw invalidResponse(
                    "FerricStore HTTP FLOW.CREATE_MANY returned an invalid number of results",
                    response,
                    null);
        }
        for (Object item : results) {
            if (item instanceof List<?> pair && pair.size() == 2) {
                String status = responseToken(pair.get(0));
                if (!"ok".equals(status)) {
                    Object detailValue = pair.get(1);
                    Map<String, Object> detail = mapOrEmpty(detailValue);
                    String code = textOr(detail.get("code"), status);
                    String message = commandErrorMessage(detail, detailValue);
                    throw commandError(message, code, detail, response);
                }
            }
        }
        return java.util.Collections.unmodifiableList(new ArrayList<>(results));
    }

    private static String commandErrorMessage(Map<String, Object> detail, Object detailValue) {
        String fallback =
                detailValue instanceof byte[] bytes
                        ? new String(bytes, StandardCharsets.UTF_8)
                        : String.valueOf(detailValue);
        return textOr(detail.get("message"), fallback);
    }

    private List<Object> decodePipelineResponse(int expectedResults, Map<String, Object> response) {
        Object rawResults = response.get("results");
        if (!(rawResults instanceof List<?> results)) {
            throw invalidResponse("FerricStore HTTP response is missing results", response, null);
        }
        if (results.size() != expectedResults) {
            throw invalidResponse(
                    "FerricStore HTTP response returned "
                            + results.size()
                            + " results; expected "
                            + expectedResults,
                    response,
                    null);
        }
        Object responseEncoding = response.get("encoding");
        String expectedEncoding =
                compact ? HttpMessagePackCodec.ENCODING : HttpBinaryEnvelope.ENCODING;
        if (responseEncoding != null && !expectedEncoding.equals(responseEncoding)) {
            throw invalidResponse(
                    "FerricStore HTTP response uses an unknown command encoding", response, null);
        }
        List<Object> decoded = new ArrayList<>(results.size());
        for (Object result : results) {
            decoded.add(decodeResult(result));
        }
        return java.util.Collections.unmodifiableList(decoded);
    }

    @Override
    public void close() {
        closed.set(true);
        client.set(null);
        requestSlots.close();
    }

    private CompletableFuture<Map<String, Object>> sendAsync(
            EncodedBody body, Duration effectiveTimeout) {
        Long timeoutNanos = effectiveTimeout == null ? null : durationToNanos(effectiveTimeout);
        long started = System.nanoTime();
        CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
        CompletableFuture<AsyncPermitPool.Permit> capacity = requestSlots.acquire(timeoutNanos);
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        capacity.cancel(false);
                    }
                });
        capacity.whenComplete(
                (permit, capacityFailure) -> {
                    if (capacityFailure != null) {
                        result.completeExceptionally(mapCapacityFailure(capacityFailure));
                        return;
                    }
                    if (result.isDone()) {
                        permit.close();
                        return;
                    }
                    sendWithPermit(body, started, timeoutNanos, permit, result);
                });
        return result;
    }

    private void sendWithPermit(
            EncodedBody body,
            long started,
            Long timeoutNanos,
            AsyncPermitPool.Permit permit,
            CompletableFuture<Map<String, Object>> result) {
        Long remaining = remainingNanos(started, timeoutNanos);
        if (remaining != null && remaining <= 0) {
            permit.close();
            result.completeExceptionally(
                    transportFailure(
                            "FerricStore HTTP request timed out waiting for client capacity",
                            "transport_timeout",
                            null,
                            RequestDelivery.NOT_SENT));
            return;
        }
        CompletableFuture<HttpResponse<byte[]>> responseFuture =
                sendFollowingRedirectsAsync(body, started, timeoutNanos, remaining);
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        responseFuture.cancel(true);
                        permit.close();
                    }
                });
        responseFuture.whenComplete(
                (response, failure) -> {
                    permit.close();
                    if (failure != null) {
                        result.completeExceptionally(sendFailure(failure));
                        return;
                    }
                    try {
                        observedVersion.set(response.version());
                        boolean messagePack = messagePackResponse(response);
                        Map<String, Object> payload =
                                response.body().length == 0
                                        ? Map.of()
                                        : decodeResponse(
                                                response.body(),
                                                response.statusCode(),
                                                messagePack);
                        if (response.statusCode() != 200) {
                            throw topLevelError(response, payload);
                        }
                        result.complete(payload);
                    } catch (RuntimeException error) {
                        result.completeExceptionally(error);
                    }
                });
    }

    private CompletableFuture<HttpResponse<byte[]>> sendFollowingRedirectsAsync(
            EncodedBody initialBody, long started, Long timeoutNanos, Long initialRemaining) {
        return sendRedirectAsync(
                commandEndpoint, "POST", initialBody, started, timeoutNanos, initialRemaining, 0);
    }

    private CompletableFuture<HttpResponse<byte[]>> sendRedirectAsync(
            URI current,
            String method,
            EncodedBody body,
            long started,
            Long timeoutNanos,
            Long remaining,
            int redirectCount) {
        try {
            String contentType = compact ? HttpMessagePackCodec.CONTENT_TYPE : "application/json";
            HttpRequest.Builder request =
                    HttpRequest.newBuilder(current).header("Accept", contentType);
            if (remaining != null) {
                request.timeout(Duration.ofNanos(remaining));
            }
            headers.forEach(request::header);
            if ("POST".equals(method)) {
                request.header("Content-Type", contentType)
                        .POST(new ImmutableByteArrayBodyPublisher(body.bytes(), body.length()));
            } else {
                request.GET();
            }
            CompletableFuture<HttpResponse<byte[]>> exchange =
                    requireOpenClient()
                            .sendAsync(request.build(), boundedBodyHandler(maxResponseBytes));
            return composeCancellable(
                    exchange,
                    response ->
                            followRedirectIfNeeded(
                                    response,
                                    current,
                                    method,
                                    body,
                                    started,
                                    timeoutNanos,
                                    redirectCount));
        } catch (RuntimeException error) {
            return AsyncFutures.failed(error);
        }
    }

    private CompletableFuture<HttpResponse<byte[]>> followRedirectIfNeeded(
            HttpResponse<byte[]> response,
            URI current,
            String method,
            EncodedBody body,
            long started,
            Long timeoutNanos,
            int redirectCount) {
        try {
            String location = response.headers().firstValue("Location").orElse(null);
            if (!isRedirect(response.statusCode()) || location == null) {
                return CompletableFuture.completedFuture(response);
            }
            URI redirect = resolveRedirect(current, location);
            if (!shouldFollowRedirect(current, redirect)) {
                return CompletableFuture.completedFuture(response);
            }
            if (redirectCount == 10) {
                return AsyncFutures.failed(new IOException("too many HTTP redirects"));
            }
            String nextMethod = method;
            EncodedBody nextBody = body;
            if (((response.statusCode() == 301 || response.statusCode() == 302)
                            && "POST".equals(method))
                    || (response.statusCode() == 303 && !"HEAD".equals(method))) {
                nextMethod = "GET";
                nextBody = EMPTY_BODY;
            }
            Long remaining = remainingNanos(started, timeoutNanos);
            if (remaining != null && remaining <= 0) {
                return AsyncFutures.failed(
                        new HttpTimeoutException("FerricStore HTTP redirect deadline exceeded"));
            }
            return sendRedirectAsync(
                    redirect,
                    nextMethod,
                    nextBody,
                    started,
                    timeoutNanos,
                    remaining,
                    redirectCount + 1);
        } catch (IOException | RuntimeException error) {
            return AsyncFutures.failed(error);
        }
    }

    private boolean shouldFollowRedirect(URI current, URI location) {
        if (redirects == HttpClient.Redirect.NEVER) {
            return false;
        }
        URI resolved = current.resolve(location);
        return redirects != HttpClient.Redirect.NORMAL
                || !"https".equalsIgnoreCase(current.getScheme())
                || !"http".equalsIgnoreCase(resolved.getScheme());
    }

    private static boolean isRedirect(int status) {
        return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
    }

    private static URI validatedRedirect(URI redirect) throws IOException {
        String scheme = redirect.getScheme();
        if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IOException("HTTP redirect target must use http:// or https://");
        }
        if (redirect.getHost() == null) {
            throw new IOException("HTTP redirect target must include a host");
        }
        return redirect;
    }

    private static URI resolveRedirect(URI current, String location) throws IOException {
        try {
            return validatedRedirect(current.resolve(location));
        } catch (IllegalArgumentException error) {
            throw new IOException("HTTP redirect Location is not a valid URI", error);
        }
    }

    private Object decodeResult(Object raw) {
        if (!(raw instanceof Map<?, ?>)) {
            throw invalidResponse(
                    "FerricStore HTTP command result is not an object", Map.of(), null);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) raw;
        String status = Resp.string(result.get("status"));
        if ("ok".equals(status)) {
            return result.get("value");
        }
        Map<String, Object> detail = mapOrEmpty(result.get("error"));
        String code = textOr(detail.get("code"), "upstream_error");
        String message = textOr(detail.get("message"), code.replace('_', ' '));
        throw commandError(message, code, detail, result);
    }

    private HttpCommandException commandError(
            String message, String code, Map<String, Object> detail, Map<String, Object> raw) {
        return new HttpCommandException(
                message,
                code,
                booleanValue(detail.get("retryable")),
                booleanValue(detail.get("safe_to_retry")),
                nonNegativeLong(detail.get("retry_after_ms")),
                raw);
    }

    private Duration effectiveRequestTimeout(List<List<Object>> commands) {
        Duration extension = Duration.ZERO;
        for (List<Object> command : commands) {
            BlockingBudget budget = blockingBudget(command);
            if (budget.disableDefault()) {
                return null;
            }
            try {
                extension = extension.plus(budget.extension());
            } catch (ArithmeticException error) {
                return null;
            }
        }
        try {
            return requestTimeout.plus(extension);
        } catch (ArithmeticException error) {
            return null;
        }
    }

    private static BlockingBudget blockingBudget(List<Object> original) {
        if (original == null || original.isEmpty()) {
            return BlockingBudget.NONE;
        }
        List<Object> command = canonicalCommand(original);
        String name = commandName(command.get(0), 0).toUpperCase(Locale.ROOT);
        List<Object> values = command.subList(1, command.size());
        Object timeout = null;
        double unitMillis = 0;
        switch (name) {
            case "BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX" -> {
                if (!values.isEmpty()) {
                    timeout = values.get(values.size() - 1);
                    unitMillis = 1_000;
                }
            }
            case "BLMPOP", "BZMPOP" -> {
                if (!values.isEmpty()) {
                    timeout = values.get(0);
                    unitMillis = 1_000;
                }
            }
            case "XREAD", "XREADGROUP" -> {
                timeout = streamBlockingTimeout(name, values);
                unitMillis = 1;
            }
            case "WAIT", "WAITAOF" -> {
                if (!values.isEmpty()) {
                    timeout = values.get(values.size() - 1);
                    unitMillis = 1;
                }
            }
            case "FLOW.CLAIM_DUE", "FLOW.SCHEDULE.FIRE_DUE" -> {
                timeout = namedBlockingTimeout(values);
                unitMillis = 1;
            }
            default -> {
                return BlockingBudget.NONE;
            }
        }
        Double amount = nonNegativeFiniteDouble(timeout);
        if (amount == null) {
            return BlockingBudget.NONE;
        }
        if (amount == 0) {
            return BlockingBudget.DISABLE_DEFAULT;
        }
        double nanoseconds = amount * unitMillis * 1_000_000d;
        if (!Double.isFinite(nanoseconds) || nanoseconds >= Long.MAX_VALUE) {
            return BlockingBudget.DISABLE_DEFAULT;
        }
        return new BlockingBudget(Duration.ofNanos((long) nanoseconds), false);
    }

    private static Object streamBlockingTimeout(String name, List<Object> values) {
        int index = 0;
        if ("XREADGROUP".equals(name)) {
            if (values.size() < 3 || !"GROUP".equalsIgnoreCase(argumentText(values.get(0)))) {
                return null;
            }
            index = 3;
        }
        while (index < values.size()) {
            String option = argumentText(values.get(index)).toUpperCase(Locale.ROOT);
            switch (option) {
                case "STREAMS" -> {
                    return null;
                }
                case "COUNT" -> index += 2;
                case "BLOCK" -> {
                    return index + 1 < values.size() ? values.get(index + 1) : null;
                }
                case "NOACK" -> {
                    if (!"XREADGROUP".equals(name)) {
                        return null;
                    }
                    index++;
                }
                default -> {
                    return null;
                }
            }
        }
        return null;
    }

    private static Object namedBlockingTimeout(List<Object> values) {
        for (int index = 0; index + 1 < values.size(); index++) {
            if ("BLOCK".equalsIgnoreCase(argumentText(values.get(index)))) {
                return values.get(index + 1);
            }
        }
        return null;
    }

    private static String argumentText(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    private static Double nonNegativeFiniteDouble(Object value) {
        if (value == null) {
            return null;
        }
        try {
            double parsed = Double.parseDouble(argumentText(value));
            return Double.isFinite(parsed) && parsed >= 0 ? parsed : null;
        } catch (NumberFormatException error) {
            return null;
        }
    }

    private record BlockingBudget(Duration extension, boolean disableDefault) {
        private static final BlockingBudget NONE = new BlockingBudget(Duration.ZERO, false);
        private static final BlockingBudget DISABLE_DEFAULT =
                new BlockingBudget(Duration.ZERO, true);
    }

    private record EncodedBody(byte[] bytes, int length) {}

    private EncodedBody encodeRequest(List<List<Object>> commands) {
        int initialBytes = initialRequestBytes(commands);
        try (RequestBuffer buffer = new RequestBuffer(initialBytes);
                JsonGenerator output = JSON.getFactory().createGenerator(buffer)) {
            output.writeStartObject();
            output.writeStringField("encoding", HttpBinaryEnvelope.ENCODING);
            output.writeArrayFieldStart("commands");
            for (int index = 0; index < commands.size(); index++) {
                writeCommand(output, commands.get(index), index);
            }
            output.writeEndArray();
            output.writeEndObject();
            output.flush();
            if (buffer.size() > maxRequestBytes) {
                throw new IllegalArgumentException(
                        "HTTP command request exceeds maxRequestBytes=" + maxRequestBytes);
            }
            return buffer.encodedBody();
        } catch (IOException error) {
            throw new IllegalArgumentException(
                    "HTTP command request is not JSON-compatible", error);
        }
    }

    private EncodedBody encodeFlowCreateManyRequest(FlowCreatePipeline.Batch batch) {
        int initialBytes = boundedInitialBytes(256L + batch.count() * 192L);
        try (RequestBuffer buffer = new RequestBuffer(initialBytes);
                JsonGenerator output = JSON.getFactory().createGenerator(buffer)) {
            output.writeStartObject();
            output.writeStringField("encoding", HttpBinaryEnvelope.ENCODING);
            output.writeArrayFieldStart("commands");
            output.writeStartObject();
            output.writeStringField("command", FlowCommand.CREATE_MANY.wireName());
            output.writeNumberField(
                    "opcode",
                    FlowCommand.CREATE_MANY
                            .nativeOpcode()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "FLOW.CREATE_MANY opcode is missing")));
            output.writeFieldName("payload");
            HttpJsonFlowCreateCodec.writePayload(output, batch);
            output.writeEndObject();
            output.writeEndArray();
            output.writeEndObject();
            output.flush();
            if (buffer.size() > maxRequestBytes) {
                throw new IllegalArgumentException(
                        "HTTP command request exceeds maxRequestBytes=" + maxRequestBytes);
            }
            return buffer.encodedBody();
        } catch (IOException error) {
            throw new IllegalArgumentException(
                    "HTTP command request is not JSON-compatible", error);
        }
    }

    private EncodedBody encodeMessagePackRequest(
            List<List<Object>> commands, FlowCreatePipeline.Batch flowCreateBatch) {
        try {
            byte[] bytes =
                    HttpMessagePackCodec.encode(
                            output -> {
                                output.packMapHeader(2);
                                output.packString("encoding");
                                output.packString(HttpMessagePackCodec.ENCODING);
                                output.packString("commands");
                                if (flowCreateBatch == null) {
                                    output.packArrayHeader(commands.size());
                                    for (int index = 0; index < commands.size(); index++) {
                                        writeMessagePackCommand(output, commands.get(index), index);
                                    }
                                } else {
                                    output.packArrayHeader(1);
                                    HttpMessagePackFlowCreateCodec.writeCommand(
                                            output, flowCreateBatch);
                                }
                            });
            if (bytes.length > maxRequestBytes) {
                throw new IllegalArgumentException(
                        "HTTP command request exceeds maxRequestBytes=" + maxRequestBytes);
            }
            return new EncodedBody(bytes, bytes.length);
        } catch (IOException error) {
            throw new IllegalArgumentException(
                    "HTTP command request is not MessagePack-compatible", error);
        }
    }

    private static void writeMessagePackCommand(
            org.msgpack.core.MessagePacker output, List<Object> command, int index)
            throws IOException {
        Objects.requireNonNull(command, "command " + index);
        if (command.isEmpty()) {
            throw new IllegalArgumentException("HTTP command " + index + " cannot be empty");
        }
        String name = commandName(command.get(0), index);
        List<Object> effectiveCommand = canonicalCommand(command);
        String effectiveName = commandName(effectiveCommand.get(0), index).toUpperCase(Locale.ROOT);
        if (CONNECTION_AFFINE_COMMANDS.contains(effectiveName)) {
            throw new IllegalArgumentException(
                    effectiveName
                            + " requires a connection-affine native TCP transport and is not "
                            + "supported through HTTP");
        }
        FlowCommandEncoder.Prepared structured =
                FlowCommandEncoder.prepare(
                        effectiveName, effectiveCommand.subList(1, effectiveCommand.size()));
        if (structured != null) {
            writeMessagePackStructuredCommand(output, structured);
            return;
        }
        output.packArrayHeader(command.size());
        output.packString(name);
        for (int argument = 1; argument < command.size(); argument++) {
            HttpMessagePackCodec.writeValue(output, command.get(argument));
        }
    }

    private static void writeMessagePackStructuredCommand(
            org.msgpack.core.MessagePacker output, FlowCommandEncoder.Prepared structured)
            throws IOException {
        output.packMapHeader(3);
        output.packString("command");
        output.packString(structured.command().wireName());
        output.packString("opcode");
        output.packInt(structured.opcode());
        output.packString("payload");
        HttpMessagePackCodec.writeValue(output, structured.payload());
    }

    private static String responseToken(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        }
        return String.valueOf(value).toLowerCase(Locale.ROOT);
    }

    private int initialRequestBytes(List<List<Object>> commands) {
        long estimate = 64L + commands.size() * 24L;
        for (List<Object> command : commands) {
            if (command == null) {
                continue;
            }
            for (Object argument : command) {
                estimate += 24L + estimatedJsonBytes(argument);
                if (estimate >= 1_048_576L) {
                    return boundedInitialBytes(estimate);
                }
            }
        }
        return boundedInitialBytes(estimate);
    }

    private static long estimatedJsonBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return 32L + (bytes.length + 2L) / 3L * 4L;
        }
        if (value instanceof ByteBuffer buffer) {
            return 32L + (buffer.remaining() + 2L) / 3L * 4L;
        }
        if (value instanceof String text) {
            return 2L + text.length();
        }
        if (value instanceof Number) {
            return 32L;
        }
        if (value instanceof Boolean) {
            return 5L;
        }
        return value == null ? 4L : 64L;
    }

    private int boundedInitialBytes(long estimate) {
        return (int) Math.min(maxRequestBytes, Math.min(1_048_576L, estimate));
    }

    private static void writeCommand(JsonGenerator output, List<Object> command, int index)
            throws IOException {
        Objects.requireNonNull(command, "command " + index);
        if (command.isEmpty()) {
            throw new IllegalArgumentException("HTTP command " + index + " cannot be empty");
        }
        String name = commandName(command.get(0), index);
        List<Object> effectiveCommand = canonicalCommand(command);
        String effectiveName = commandName(effectiveCommand.get(0), index).toUpperCase(Locale.ROOT);
        if (CONNECTION_AFFINE_COMMANDS.contains(effectiveName)) {
            throw new IllegalArgumentException(
                    effectiveName
                            + " requires a connection-affine native TCP transport and is not "
                            + "supported through HTTP");
        }
        FlowCommandEncoder.Prepared structured =
                FlowCommandEncoder.prepare(
                        effectiveName, effectiveCommand.subList(1, effectiveCommand.size()));
        if (structured != null) {
            writeStructuredCommand(output, structured);
            return;
        }
        output.writeStartArray();
        output.writeString(name);
        for (int argument = 1; argument < command.size(); argument++) {
            HttpBinaryEnvelope.writeJson(output, command.get(argument));
        }
        output.writeEndArray();
    }

    private static void writeStructuredCommand(
            JsonGenerator output, FlowCommandEncoder.Prepared structured) throws IOException {
        output.writeStartObject();
        output.writeStringField("command", structured.command().wireName());
        output.writeNumberField("opcode", structured.opcode());
        output.writeFieldName("payload");
        HttpBinaryEnvelope.writeJson(output, structured.payload());
        output.writeEndObject();
    }

    private static List<Object> canonicalCommand(List<Object> original) {
        List<Object> command = original;
        while (command.size() > 1
                && "COMMAND_EXEC".equalsIgnoreCase(commandName(command.get(0), 0))) {
            command = command.subList(1, command.size());
        }
        return command;
    }

    private static String commandName(Object value, int index) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        if (value instanceof byte[] bytes) {
            try {
                String text =
                        StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(bytes))
                                .toString();
                if (!text.isBlank()) {
                    return text;
                }
            } catch (CharacterCodingException error) {
                throw new IllegalArgumentException(
                        "HTTP command " + index + " name must be valid UTF-8", error);
            }
        }
        throw new IllegalArgumentException("HTTP command " + index + " name must be text");
    }

    private static URI commandEndpoint(String endpoint, HttpTransportOptions options) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("HTTP endpoint must not be blank");
        }
        URI base;
        try {
            base = new URI(endpoint);
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("invalid FerricStore HTTP endpoint", error);
        }
        String scheme = base.getScheme() == null ? "" : base.getScheme().toLowerCase(Locale.ROOT);
        if (!("http".equals(scheme) || "https".equals(scheme))) {
            throw new IllegalArgumentException("HTTP endpoint must use http:// or https://");
        }
        if (base.getHost() == null) {
            throw new IllegalArgumentException("HTTP endpoint must include a host");
        }
        if (base.getRawUserInfo() != null) {
            throw new IllegalArgumentException(
                    "HTTP credentials must use HttpTransportOptions, not URL user info");
        }
        if (base.getRawQuery() != null || base.getRawFragment() != null) {
            throw new IllegalArgumentException("HTTP endpoint cannot contain a query or fragment");
        }
        if ("http".equals(scheme)
                && (options.username() != null || options.password() != null)
                && !options.allowInsecureBasicAuthentication()) {
            throw new IllegalArgumentException(
                    "HTTP Basic credentials require https://; set "
                            + "allowInsecureBasicAuthentication(true) only behind a trusted TLS ingress");
        }
        String normalized =
                endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return URI.create(normalized + "/v1/commands");
    }

    private static HttpClient createClient(HttpTransportOptions options) {
        if (options.httpClient() != null) {
            if (options.httpClient().followRedirects() != HttpClient.Redirect.NEVER) {
                throw new IllegalArgumentException(
                        "a custom HttpClient must disable automatic redirects so the SDK can "
                                + "preserve authentication and one absolute deadline");
            }
            return options.httpClient();
        }
        HttpClient.Builder builder =
                HttpClient.newBuilder()
                        .connectTimeout(options.connectTimeout())
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .version(options.preferredVersion());
        SSLContext sslContext = options.sslContext();
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        return builder.build();
    }

    private static Map<String, String> authenticationHeaders(HttpTransportOptions options) {
        Map<String, String> result = new LinkedHashMap<>(options.headers());
        if (options.bearerToken() != null) {
            result.put("Authorization", "Bearer " + options.bearerToken());
        } else if (options.password() != null) {
            String username = options.username() == null ? "default" : options.username();
            String credentials = username + ":" + options.password();
            result.put(
                    "Authorization",
                    "Basic "
                            + Base64.getEncoder()
                                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
        }
        return Map.copyOf(result);
    }

    private HttpClient requireOpenClient() {
        HttpClient current = client.get();
        if (closed.get() || current == null) {
            throw new IllegalStateException("HTTP executor is closed");
        }
        return current;
    }

    private void requireOpen() {
        requireOpenClient();
    }

    private static boolean messagePackResponse(HttpResponse<?> response) {
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        int parameter = contentType.indexOf(';');
        String mediaType = parameter < 0 ? contentType : contentType.substring(0, parameter);
        return HttpMessagePackCodec.CONTENT_TYPE.equalsIgnoreCase(mediaType.trim());
    }

    private static Map<String, Object> decodeResponse(
            byte[] body, int statusCode, boolean messagePack) {
        try {
            return messagePack
                    ? HttpMessagePackCodec.decodeResponse(body)
                    : HttpResponseDecoder.decode(body);
        } catch (HttpResponseDecoder.MalformedEnvelopeException error) {
            throw invalidResponse(
                    "FerricStore HTTP response contains a malformed binary value", Map.of(), error);
        } catch (IOException error) {
            throw new HttpTransportException(
                    "FerricStore HTTP endpoint returned malformed "
                            + (messagePack ? "MessagePack" : "JSON"),
                    statusCode,
                    "invalid_response",
                    false,
                    false,
                    null,
                    Map.of(),
                    error);
        }
    }

    private static HttpTransportException topLevelError(
            HttpResponse<byte[]> response, Map<String, Object> payload) {
        Map<String, Object> detail = mapOrEmpty(payload.get("error"));
        String code = textOr(detail.get("code"), "http_" + response.statusCode());
        String message = textOr(detail.get("message"), code.replace('_', ' '));
        Long retryAfter = nonNegativeLong(detail.get("retry_after_ms"));
        if (retryAfter == null) {
            retryAfter = retryAfterMs(response);
        }
        return new HttpTransportException(
                message,
                response.statusCode(),
                code,
                booleanValue(detail.get("retryable")),
                booleanValue(detail.get("safe_to_retry")),
                retryAfter,
                payload,
                null);
    }

    private static HttpTransportException transportFailure(
            String message, String code, Throwable cause) {
        return transportFailure(message, code, cause, RequestDelivery.UNKNOWN);
    }

    private static HttpTransportException localFailure(RuntimeException failure) {
        if (failure instanceof HttpTransportException transport) {
            return transport;
        }
        return transportFailure(
                "FerricStore HTTP request was rejected before submission: " + failure.getMessage(),
                "client_request_invalid",
                failure,
                RequestDelivery.NOT_SENT);
    }

    private static HttpTransportException transportFailure(
            String message, String code, Throwable cause, RequestDelivery delivery) {
        return new HttpTransportException(
                message, 0, code, false, false, null, Map.of(), cause, delivery);
    }

    private RuntimeException mapCapacityFailure(Throwable failure) {
        Throwable error = AsyncFutures.unwrap(failure);
        if (error instanceof TimeoutException) {
            return transportFailure(
                    "FerricStore HTTP request timed out waiting for client capacity",
                    "transport_timeout",
                    error,
                    RequestDelivery.NOT_SENT);
        }
        if (error instanceof RejectedExecutionException) {
            return transportFailure(
                    "FerricStore HTTP client pending request limit exceeded",
                    "client_overloaded",
                    error,
                    RequestDelivery.NOT_SENT);
        }
        if (error instanceof RuntimeException runtime) {
            return runtime;
        }
        return transportFailure(
                "FerricStore HTTP request failed while waiting for client capacity",
                "transport_error",
                error,
                RequestDelivery.NOT_SENT);
    }

    private static <S, T> CompletableFuture<T> composeCancellable(
            CompletableFuture<S> source,
            Function<? super S, ? extends CompletableFuture<T>> continuation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<T>> next = new AtomicReference<>();
        result.whenComplete(
                (ignored, failure) -> {
                    if (result.isCancelled()) {
                        source.cancel(true);
                        CompletableFuture<T> current = next.get();
                        if (current != null) {
                            current.cancel(true);
                        }
                    }
                });
        source.whenComplete(
                (value, failure) -> {
                    if (failure != null) {
                        result.completeExceptionally(AsyncFutures.unwrap(failure));
                        return;
                    }
                    if (result.isDone()) {
                        return;
                    }
                    CompletableFuture<T> current;
                    try {
                        current = Objects.requireNonNull(continuation.apply(value));
                    } catch (RuntimeException error) {
                        result.completeExceptionally(error);
                        return;
                    }
                    next.set(current);
                    if (result.isCancelled()) {
                        current.cancel(true);
                        return;
                    }
                    current.whenComplete(
                            (continuedValue, continuedFailure) -> {
                                if (continuedFailure != null) {
                                    result.completeExceptionally(
                                            AsyncFutures.unwrap(continuedFailure));
                                } else {
                                    result.complete(continuedValue);
                                }
                            });
                });
        return result;
    }

    private RuntimeException sendFailure(Throwable failure) {
        Throwable error = AsyncFutures.unwrap(failure);
        if (error instanceof HttpTimeoutException || error instanceof TimeoutException) {
            return transportFailure(
                    "FerricStore HTTP request timed out; outcome is unknown",
                    "transport_timeout",
                    error);
        }
        if (causedBy(error, ResponseTooLargeException.class)) {
            return transportFailure(
                    "FerricStore HTTP response exceeds maxResponseBytes=" + maxResponseBytes,
                    "response_too_large",
                    error);
        }
        if (error instanceof HttpTransportException transport) {
            return transport;
        }
        if (error instanceof IOException) {
            return transportFailure(
                    "FerricStore HTTP request failed after submission; outcome is unknown",
                    "transport_error",
                    error);
        }
        if (error instanceof RuntimeException runtime) {
            return runtime;
        }
        return transportFailure(
                "FerricStore HTTP request failed after submission; outcome is unknown",
                "transport_error",
                error);
    }

    private static HttpTransportException invalidResponse(
            String message, Map<String, Object> raw, Throwable cause) {
        return new HttpTransportException(
                message, 200, "invalid_response", false, false, null, raw, cause);
    }

    private static Map<String, Object> mapOrEmpty(Object value) {
        if (!(value instanceof Map<?, ?>)) {
            return Map.of();
        }
        return Resp.map(value);
    }

    private static String textOr(Object value, String fallback) {
        return value instanceof String text && !text.isBlank() ? text : fallback;
    }

    private static boolean booleanValue(Object value) {
        return Boolean.TRUE.equals(value);
    }

    private static Long nonNegativeLong(Object value) {
        if (value instanceof Number number && number.longValue() >= 0) {
            return number.longValue();
        }
        return null;
    }

    private static Long retryAfterMs(HttpResponse<?> response) {
        String value = response.headers().firstValue("Retry-After").orElse(null);
        if (value == null) {
            return null;
        }
        try {
            long seconds = Long.parseLong(value);
            return seconds < 0 ? null : Math.multiplyExact(seconds, 1_000L);
        } catch (NumberFormatException | ArithmeticException ignored) {
            return null;
        }
    }

    private static long durationToNanos(Duration value) {
        try {
            return value.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static Long remainingNanos(long started, Long timeoutNanos) {
        if (timeoutNanos == null) {
            return null;
        }
        long elapsed = System.nanoTime() - started;
        return elapsed >= timeoutNanos ? 0L : timeoutNanos - elapsed;
    }

    private static boolean causedBy(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static HttpResponse.BodyHandler<byte[]> boundedBodyHandler(int maxBytes) {
        return responseInfo -> {
            long declaredBytes =
                    responseInfo.headers().firstValueAsLong("Content-Length").orElse(-1L);
            boolean declaredTooLarge = declaredBytes > maxBytes;
            int expectedBytes =
                    declaredBytes < 0 || declaredTooLarge ? -1 : Math.toIntExact(declaredBytes);
            return new BoundedBodySubscriber(maxBytes, declaredTooLarge, expectedBytes);
        };
    }

    private static final class RequestBuffer extends ByteArrayOutputStream {
        private RequestBuffer(int initialBytes) {
            super(initialBytes);
        }

        private EncodedBody encodedBody() {
            return new EncodedBody(buf, count);
        }
    }

    private static final class BoundedBodySubscriber
            implements HttpResponse.BodySubscriber<byte[]> {
        private final int maxBytes;
        private final boolean reject;
        private final CompletableFuture<byte[]> body = new CompletableFuture<>();
        private final List<byte[]> chunks;
        private final byte[] expectedBody;
        private Flow.Subscription subscription;
        private int receivedBytes;

        private BoundedBodySubscriber(int maxBytes, boolean reject, int expectedBytes) {
            this.maxBytes = maxBytes;
            this.reject = reject;
            expectedBody = expectedBytes < 0 ? null : new byte[expectedBytes];
            chunks = expectedBody == null ? new ArrayList<>() : List.of();
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return body.minimalCompletionStage();
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            if (reject) {
                value.cancel();
                body.completeExceptionally(new ResponseTooLargeException());
            } else {
                value.request(1);
            }
        }

        @Override
        @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
        public void onNext(List<ByteBuffer> buffers) {
            Flow.Subscription activeSubscription = subscription;
            if (activeSubscription == null) {
                body.completeExceptionally(
                        new IllegalStateException("HTTP body arrived before subscription"));
                return;
            }
            for (ByteBuffer buffer : buffers) {
                int chunkBytes = buffer.remaining();
                if (chunkBytes > maxBytes - receivedBytes) {
                    activeSubscription.cancel();
                    body.completeExceptionally(new ResponseTooLargeException());
                    return;
                }
                if (expectedBody != null && chunkBytes > expectedBody.length - receivedBytes) {
                    activeSubscription.cancel();
                    body.completeExceptionally(
                            new IOException("HTTP response exceeds its Content-Length"));
                    return;
                }
                if (expectedBody != null) {
                    buffer.get(expectedBody, receivedBytes, chunkBytes);
                } else {
                    byte[] chunk = new byte[chunkBytes];
                    buffer.get(chunk);
                    chunks.add(chunk);
                }
                receivedBytes += chunkBytes;
            }
            activeSubscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            body.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            if (expectedBody != null) {
                if (receivedBytes != expectedBody.length) {
                    body.completeExceptionally(
                            new IOException("HTTP response ended before Content-Length bytes"));
                } else {
                    body.complete(expectedBody);
                }
                return;
            }
            if (chunks.isEmpty()) {
                body.complete(new byte[0]);
                return;
            }
            if (chunks.size() == 1) {
                body.complete(chunks.get(0));
                return;
            }
            byte[] result = new byte[receivedBytes];
            int offset = 0;
            for (byte[] chunk : chunks) {
                System.arraycopy(chunk, 0, result, offset, chunk.length);
                offset += chunk.length;
            }
            body.complete(result);
        }
    }

    private static final class ResponseTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;
    }
}
