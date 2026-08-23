package com.ferricstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;

/** Ordered, binary-safe HTTP/HTTPS executor for FerricStore's stateless command endpoint. */
public final class HttpExecutor implements CommandExecutor, AutoCloseable {
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
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
    private final Semaphore requestSlots;
    private final AtomicBoolean closed = new AtomicBoolean();

    private HttpExecutor(String endpoint, HttpTransportOptions options) {
        commandEndpoint = commandEndpoint(endpoint, options);
        client = new AtomicReference<>(createClient(options));
        headers = authenticationHeaders(options);
        requestTimeout = options.requestTimeout();
        maxRequestBytes = options.maxRequestBytes();
        maxResponseBytes = options.maxResponseBytes();
        maxBatchItems = options.maxBatchItems();
        redirects = options.redirects();
        requestSlots = new Semaphore(options.maxConcurrentRequests());
    }

    public static HttpExecutor connect(String endpoint) {
        return connect(endpoint, HttpTransportOptions.defaults());
    }

    public static HttpExecutor connect(String endpoint, HttpTransportOptions options) {
        Objects.requireNonNull(options, "HTTP transport options");
        return new HttpExecutor(endpoint, options);
    }

    @Override
    public Object execute(List<Object> args) {
        return pipeline(List.of(args)).get(0);
    }

    @Override
    public List<Object> pipeline(List<List<Object>> commands) {
        Objects.requireNonNull(commands, "commands");
        requireOpen();
        if (commands.isEmpty()) {
            return List.of();
        }
        if (commands.size() > maxBatchItems) {
            throw new IllegalArgumentException(
                    "HTTP command batch exceeds maxBatchItems=" + maxBatchItems);
        }

        List<Object> encoded = new ArrayList<>(commands.size());
        for (int index = 0; index < commands.size(); index++) {
            encoded.add(encodeCommand(commands.get(index), index));
        }
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("encoding", HttpBinaryEnvelope.ENCODING);
        envelope.put("commands", encoded);
        byte[] requestBody = encodeRequest(envelope);
        Map<String, Object> response = send(requestBody, effectiveRequestTimeout(commands));
        Object rawResults = response.get("results");
        if (!(rawResults instanceof List<?> results)) {
            throw invalidResponse("FerricStore HTTP response is missing results", response, null);
        }
        if (results.size() != commands.size()) {
            throw invalidResponse(
                    "FerricStore HTTP response returned "
                            + results.size()
                            + " results; expected "
                            + commands.size(),
                    response,
                    null);
        }
        Object responseEncoding = response.get("encoding");
        if (responseEncoding != null && !HttpBinaryEnvelope.ENCODING.equals(responseEncoding)) {
            throw invalidResponse(
                    "FerricStore HTTP response uses an unknown command encoding", response, null);
        }
        return results.stream().map(this::decodeResult).toList();
    }

    @Override
    public void close() {
        closed.set(true);
        client.set(null);
    }

    private Map<String, Object> send(byte[] body, Duration effectiveTimeout) {
        boolean acquired = false;
        Long timeoutNanos = effectiveTimeout == null ? null : durationToNanos(effectiveTimeout);
        long started = System.nanoTime();
        try {
            if (timeoutNanos == null) {
                requestSlots.acquire();
                acquired = true;
            } else {
                acquired =
                        requestSlots.tryAcquire(
                                timeoutNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
            }
            if (!acquired) {
                throw transportFailure(
                        "FerricStore HTTP request timed out waiting for client capacity",
                        "transport_timeout",
                        null);
            }
            long elapsed = System.nanoTime() - started;
            Long remaining = timeoutNanos == null ? null : Math.max(1L, timeoutNanos - elapsed);
            HttpResponse<byte[]> response =
                    sendFollowingRedirects(body, started, timeoutNanos, remaining);
            Map<String, Object> payload =
                    response.body().length == 0
                            ? Map.of()
                            : decodeResponse(response.body(), response.statusCode());
            if (response.statusCode() != 200) {
                throw topLevelError(response, payload);
            }
            return payload;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw transportFailure(
                    "FerricStore HTTP request was interrupted; outcome is unknown",
                    "transport_interrupted",
                    error);
        } catch (HttpTimeoutException error) {
            throw transportFailure(
                    "FerricStore HTTP request timed out; outcome is unknown",
                    "transport_timeout",
                    error);
        } catch (IOException error) {
            if (causedBy(error, ResponseTooLargeException.class)) {
                throw transportFailure(
                        "FerricStore HTTP response exceeds maxResponseBytes=" + maxResponseBytes,
                        "response_too_large",
                        error);
            }
            throw transportFailure(
                    "FerricStore HTTP request failed after submission; outcome is unknown",
                    "transport_error",
                    error);
        } finally {
            if (acquired) {
                requestSlots.release();
            }
        }
    }

    private HttpResponse<byte[]> sendFollowingRedirects(
            byte[] initialBody, long started, Long timeoutNanos, Long initialRemaining)
            throws IOException, InterruptedException {
        URI current = commandEndpoint;
        String method = "POST";
        byte[] body = initialBody;
        Long remaining = initialRemaining;
        int redirectCount = 0;
        while (true) {
            HttpRequest.Builder request =
                    HttpRequest.newBuilder(current).header("Accept", "application/json");
            if (remaining != null) {
                request.timeout(Duration.ofNanos(remaining));
            }
            headers.forEach(request::header);
            if ("POST".equals(method)) {
                request.header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            } else {
                request.GET();
            }
            HttpResponse<byte[]> response =
                    requireOpenClient().send(request.build(), boundedBodyHandler(maxResponseBytes));
            String location = response.headers().firstValue("Location").orElse(null);
            if (!isRedirect(response.statusCode()) || location == null) {
                return response;
            }
            URI redirect = resolveRedirect(current, location);
            if (!shouldFollowRedirect(current, redirect)) {
                return response;
            }
            if (redirectCount == 10) {
                throw new IOException("too many HTTP redirects");
            }
            redirectCount++;
            current = redirect;
            if (((response.statusCode() == 301 || response.statusCode() == 302)
                            && "POST".equals(method))
                    || (response.statusCode() == 303 && !"HEAD".equals(method))) {
                method = "GET";
                body = EMPTY_BODY;
            }
            if (timeoutNanos != null) {
                long elapsed = System.nanoTime() - started;
                remaining = Math.max(1L, timeoutNanos - elapsed);
                if (elapsed >= timeoutNanos) {
                    throw new HttpTimeoutException("FerricStore HTTP redirect deadline exceeded");
                }
            }
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
        Map<String, Object> result;
        try {
            result = Resp.map(raw);
        } catch (RuntimeException error) {
            throw invalidResponse(
                    "FerricStore HTTP command result is not an object", Map.of(), error);
        }
        String status = Resp.string(result.get("status"));
        if ("ok".equals(status)) {
            try {
                return HttpBinaryEnvelope.decode(result.get("value"));
            } catch (RuntimeException error) {
                throw invalidResponse(
                        "FerricStore HTTP response contains a malformed binary value",
                        result,
                        error);
            }
        }
        Map<String, Object> detail = mapOrEmpty(result.get("error"));
        String code = textOr(detail.get("code"), "upstream_error");
        String message = textOr(detail.get("message"), code.replace('_', ' '));
        return throwCommandError(message, code, detail, result);
    }

    private Object throwCommandError(
            String message, String code, Map<String, Object> detail, Map<String, Object> raw) {
        throw new HttpCommandException(
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

    private byte[] encodeRequest(Map<String, Object> envelope) {
        try {
            byte[] bytes = JSON.writeValueAsBytes(envelope);
            if (bytes.length > maxRequestBytes) {
                throw new IllegalArgumentException(
                        "HTTP command request exceeds maxRequestBytes=" + maxRequestBytes);
            }
            return bytes;
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException(
                    "HTTP command request is not JSON-compatible", error);
        }
    }

    private static Object encodeCommand(List<Object> command, int index) {
        Objects.requireNonNull(command, "command " + index);
        if (command.isEmpty()) {
            throw new IllegalArgumentException("HTTP command " + index + " cannot be empty");
        }
        String name = commandName(command.get(0), index);
        String normalized = name.toUpperCase(Locale.ROOT);
        List<Object> effectiveCommand = canonicalCommand(command);
        String effectiveName = commandName(effectiveCommand.get(0), index).toUpperCase(Locale.ROOT);
        if (CONNECTION_AFFINE_COMMANDS.contains(effectiveName)) {
            throw new IllegalArgumentException(
                    effectiveName
                            + " requires a connection-affine native TCP transport and is not "
                            + "supported through HTTP");
        }
        FlowCommandEncoder.Prepared structured =
                FlowCommandEncoder.prepare(normalized, command.subList(1, command.size()));
        if (structured != null) {
            Map<String, Object> descriptor = new LinkedHashMap<>();
            descriptor.put("command", structured.command().wireName());
            descriptor.put("opcode", structured.opcode());
            descriptor.put("payload", HttpBinaryEnvelope.encode(structured.payload()));
            return descriptor;
        }
        List<Object> encoded = new ArrayList<>(command.size());
        encoded.add(name);
        for (int argument = 1; argument < command.size(); argument++) {
            encoded.add(HttpBinaryEnvelope.encode(command.get(argument)));
        }
        return encoded;
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

    private static Map<String, Object> decodeResponse(byte[] body, int statusCode) {
        try {
            return JSON.readValue(body, JSON_OBJECT);
        } catch (IOException error) {
            throw new HttpTransportException(
                    "FerricStore HTTP endpoint returned malformed JSON",
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
        return new HttpTransportException(message, 0, code, false, false, null, Map.of(), cause);
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
            boolean declaredTooLarge =
                    responseInfo.headers().firstValueAsLong("Content-Length").stream()
                            .anyMatch(length -> length > maxBytes);
            return new BoundedBodySubscriber(maxBytes, declaredTooLarge);
        };
    }

    private static final class BoundedBodySubscriber
            implements HttpResponse.BodySubscriber<byte[]> {
        private final int maxBytes;
        private final boolean reject;
        private final CompletableFuture<byte[]> body = new CompletableFuture<>();
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private Flow.Subscription subscription;

        private BoundedBodySubscriber(int maxBytes, boolean reject) {
            this.maxBytes = maxBytes;
            this.reject = reject;
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
                if (buffer.remaining() > maxBytes - output.size()) {
                    activeSubscription.cancel();
                    body.completeExceptionally(new ResponseTooLargeException());
                    return;
                }
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                output.writeBytes(chunk);
            }
            activeSubscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            body.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            body.complete(output.toByteArray());
        }
    }

    private static final class ResponseTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;
    }
}
