package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class HttpExecutorTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void executeAsyncDoesNotBlockTheCallerAndRunsHttpRequestsConcurrently() throws Exception {
        CountDownLatch arrived = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try (TestServer server =
                        server(
                                exchange -> {
                                    Map<String, Object> request = readJson(exchange);
                                    arrived.countDown();
                                    try {
                                        if (!release.await(5, TimeUnit.SECONDS)) {
                                            throw new IOException("async requests did not overlap");
                                        }
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException(
                                                "HTTP test server interrupted", error);
                                    }
                                    List<?> command =
                                            (List<?>) ((List<?>) request.get("commands")).get(0);
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of(
                                                                    "status",
                                                                    "ok",
                                                                    "value",
                                                                    command.get(1)))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            CompletableFuture<Object> alpha =
                    executor.executeAsync(List.of("ECHO", bytes("alpha")));
            CompletableFuture<Object> beta = executor.executeAsync(List.of("ECHO", bytes("beta")));

            assertTrue(arrived.await(5, TimeUnit.SECONDS));
            assertFalse(alpha.isDone());
            assertFalse(beta.isDone());
            release.countDown();
            assertArrayEquals(bytes("alpha"), (byte[]) alpha.get(5, TimeUnit.SECONDS));
            assertArrayEquals(bytes("beta"), (byte[]) beta.get(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    void asyncCapacityWaitUsesTheRequestDeadlineWithoutBlockingOrSending() throws Exception {
        CountDownLatch arrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        try (TestServer server =
                        server(
                                exchange -> {
                                    requests.incrementAndGet();
                                    readJson(exchange);
                                    arrived.countDown();
                                    try {
                                        if (!release.await(5, TimeUnit.SECONDS)) {
                                            throw new IOException("held request was not released");
                                        }
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException(
                                                "HTTP test server interrupted", error);
                                    }
                                    replyOk(exchange, "ready");
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofMillis(100))
                                        .maxConcurrentRequests(1)
                                        .build())) {
            CompletableFuture<Object> held = executor.executeAsync(List.of("BLPOP", "queue", 1));
            assertTrue(arrived.await(5, TimeUnit.SECONDS));

            CompletableFuture<Object> queued = executor.executeAsync(List.of("PING"));
            ExecutionException failure =
                    assertThrows(ExecutionException.class, () -> queued.get(2, TimeUnit.SECONDS));
            HttpTransportException timeout =
                    assertInstanceOf(HttpTransportException.class, failure.getCause());
            assertEquals("transport_timeout", timeout.errorCode());
            assertTrue(timeout.getMessage().contains("client capacity"));
            assertEquals(1, requests.get());

            release.countDown();
            assertArrayEquals(bytes("ready"), (byte[]) held.get(2, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    void rejectsRequestsBeyondTheBoundedCapacityQueue() throws Exception {
        CountDownLatch firstArrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        try (TestServer server = capacityServer(firstArrived, release, requests);
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(5))
                                        .maxConcurrentRequests(1)
                                        .maxPendingRequests(1)
                                        .build())) {
            CompletableFuture<Object> held = executor.executeAsync(List.of("PING"));
            assertTrue(firstArrived.await(5, TimeUnit.SECONDS));
            CompletableFuture<Object> queued = executor.executeAsync(List.of("PING"));
            CompletableFuture<Object> overflow = executor.executeAsync(List.of("PING"));

            ExecutionException failure =
                    assertThrows(ExecutionException.class, () -> overflow.get(1, TimeUnit.SECONDS));
            HttpTransportException overloaded =
                    assertInstanceOf(HttpTransportException.class, failure.getCause());
            assertEquals("client_overloaded", overloaded.errorCode());
            assertEquals(1, requests.get());

            release.countDown();
            assertArrayEquals(bytes("reply-1"), (byte[]) held.get(5, TimeUnit.SECONDS));
            assertArrayEquals(bytes("reply-2"), (byte[]) queued.get(5, TimeUnit.SECONDS));
            assertEquals(2, requests.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void cancellingAQueuedRequestReleasesItsCapacityWaiter() throws Exception {
        CountDownLatch firstArrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        try (TestServer server = capacityServer(firstArrived, release, requests);
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(5))
                                        .maxConcurrentRequests(1)
                                        .build())) {
            CompletableFuture<Object> held = executor.executeAsync(List.of("PING"));
            assertTrue(firstArrived.await(5, TimeUnit.SECONDS));
            CompletableFuture<Object> cancelled = executor.executeAsync(List.of("PING"));
            CompletableFuture<Object> next = executor.executeAsync(List.of("PING"));

            assertTrue(cancelled.cancel(false));
            release.countDown();

            assertArrayEquals(bytes("reply-1"), (byte[]) held.get(5, TimeUnit.SECONDS));
            assertArrayEquals(bytes("reply-2"), (byte[]) next.get(5, TimeUnit.SECONDS));
            assertEquals(2, requests.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void genericClientUsesBinarySafeHttpKeepAliveAndOneRequestPerPipeline() throws IOException {
        List<Integer> remotePorts = new CopyOnWriteArrayList<>();
        List<Map<String, Object>> requests = new CopyOnWriteArrayList<>();
        List<String> authorizations = new CopyOnWriteArrayList<>();
        try (TestServer server =
                server(
                        exchange -> {
                            remotePorts.add(exchange.getRemoteAddress().getPort());
                            authorizations.add(
                                    exchange.getRequestHeaders().getFirst("Authorization"));
                            Map<String, Object> request = readJson(exchange);
                            requests.add(request);
                            List<?> commands = (List<?>) request.get("commands");
                            List<Object> results = new ArrayList<>();
                            for (Object rawCommand : commands) {
                                List<?> command = (List<?>) rawCommand;
                                Object value =
                                        command.size() > 1
                                                ? command.get(1)
                                                : bytesMarker(
                                                        "PONG".getBytes(StandardCharsets.UTF_8));
                                results.add(Map.of("status", "ok", "value", value));
                            }
                            replyJson(
                                    exchange,
                                    200,
                                    Map.of("encoding", "ferricstore-json-v1", "results", results));
                        })) {
            HttpTransportOptions options =
                    HttpTransportOptions.builder()
                            .bearerToken("secret")
                            .preferredVersion(HttpClient.Version.HTTP_1_1)
                            .build();
            try (FerricStoreClient client =
                    FerricStoreClient.connect(server.url(), new RawCodec(), options)) {
                assertArrayEquals(bytes("PONG"), (byte[]) client.command("PING"));
                assertArrayEquals(
                        bytes("binary"), (byte[]) client.command("ECHO", bytes("binary")));
                List<Object> values =
                        client.pipeline(
                                List.of(
                                        List.of("ECHO", bytes("one")),
                                        List.of("ECHO", bytes("two"))));
                assertArrayEquals(bytes("one"), (byte[]) values.get(0));
                assertArrayEquals(bytes("two"), (byte[]) values.get(1));
            }

            assertEquals(3, requests.size());
            assertEquals(2, ((List<?>) requests.get(2).get("commands")).size());
            assertTrue(
                    requests.stream()
                            .allMatch(
                                    request ->
                                            "ferricstore-json-v1".equals(request.get("encoding"))));
            assertTrue(authorizations.stream().allMatch("Bearer secret"::equals));
            assertEquals(1, remotePorts.stream().distinct().count());
        }
    }

    @Test
    void compactClientUsesMessagePackMediaTypeAndPreservesBinaryValues() throws IOException {
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> accept = new AtomicReference<>();
        AtomicReference<Map<String, Object>> captured = new AtomicReference<>();
        try (TestServer server =
                        server(
                                exchange -> {
                                    contentType.set(
                                            exchange.getRequestHeaders().getFirst("Content-Type"));
                                    accept.set(exchange.getRequestHeaders().getFirst("Accept"));
                                    Map<String, Object> request =
                                            HttpMessagePackCodec.decode(
                                                    exchange.getRequestBody().readAllBytes());
                                    captured.set(request);
                                    List<?> commands = (List<?>) request.get("commands");
                                    List<?> command = (List<?>) commands.get(0);
                                    byte[] response =
                                            HttpMessagePackCodec.encode(
                                                    output ->
                                                            HttpMessagePackCodec.writeValue(
                                                                    output,
                                                                    Map.of(
                                                                            "encoding",
                                                                            HttpMessagePackCodec
                                                                                    .ENCODING,
                                                                            "results",
                                                                            List.of(
                                                                                    Map.of(
                                                                                            "status",
                                                                                            "ok",
                                                                                            "value",
                                                                                            command
                                                                                                    .get(
                                                                                                            1))))));
                                    exchange.getResponseHeaders()
                                            .set("Content-Type", HttpMessagePackCodec.CONTENT_TYPE);
                                    exchange.sendResponseHeaders(200, response.length);
                                    exchange.getResponseBody().write(response);
                                    exchange.close();
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder().compact(true).build())) {
            byte[] binary = {0, (byte) 0xff, (byte) 0x80};
            assertArrayEquals(binary, (byte[]) executor.execute(List.of("ECHO", binary)));
        }

        assertEquals(HttpMessagePackCodec.CONTENT_TYPE, contentType.get());
        assertEquals(HttpMessagePackCodec.CONTENT_TYPE, accept.get());
        assertEquals(HttpMessagePackCodec.ENCODING, captured.get().get("encoding"));
        List<?> commands = (List<?>) captured.get().get("commands");
        List<?> command = (List<?>) commands.get(0);
        assertEquals("ECHO", command.get(0));
        assertArrayEquals(
                new byte[] {0, (byte) 0xff, (byte) 0x80},
                assertInstanceOf(byte[].class, command.get(1)));
    }

    @Test
    void assemblesFragmentedHttpResponsesWithoutChangingTheirBinaryEnvelope() throws IOException {
        try (TestServer server =
                        server(
                                exchange -> {
                                    byte[] response =
                                            JSON.writeValueAsBytes(
                                                    Map.of(
                                                            "encoding",
                                                            "ferricstore-json-v1",
                                                            "results",
                                                            List.of(
                                                                    Map.of(
                                                                            "status",
                                                                            "ok",
                                                                            "value",
                                                                            bytesMarker(
                                                                                    bytes(
                                                                                            "fragmented"))))));
                                    int split = response.length / 2;
                                    exchange.sendResponseHeaders(200, 0);
                                    exchange.getResponseBody().write(response, 0, split);
                                    exchange.getResponseBody().flush();
                                    exchange.getResponseBody()
                                            .write(response, split, response.length - split);
                                    exchange.close();
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            assertArrayEquals(bytes("fragmented"), (byte[]) executor.execute(List.of("PING")));
        }
    }

    @Test
    void decodesNestedBinaryResponseValuesWithoutLeakingEnvelopeObjects() throws IOException {
        Map<Object, Object> nested = new java.util.LinkedHashMap<>();
        nested.put(new byte[] {0, (byte) 0xff}, List.of(bytes("value"), 42));
        try (TestServer server =
                        server(
                                exchange -> {
                                    readJson(exchange);
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of(
                                                                    "status",
                                                                    "ok",
                                                                    "value",
                                                                    HttpBinaryEnvelope.encode(
                                                                            nested)))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            Map<?, ?> decoded = assertInstanceOf(Map.class, executor.execute(List.of("PING")));
            assertEquals(1, decoded.size());
            Map.Entry<?, ?> entry = decoded.entrySet().iterator().next();
            byte[] key = assertInstanceOf(byte[].class, entry.getKey());
            assertArrayEquals(new byte[] {0, (byte) 0xff}, key);
            List<?> values = assertInstanceOf(List.class, entry.getValue());
            assertArrayEquals(bytes("value"), assertInstanceOf(byte[].class, values.get(0)));
            assertEquals(42, values.get(1));
        }
    }

    @Test
    void rejectsMalformedBinaryResponseMarkersAsInvalidResponses() throws IOException {
        try (TestServer server =
                        server(
                                exchange ->
                                        replyJson(
                                                exchange,
                                                200,
                                                Map.of(
                                                        "results",
                                                        List.of(
                                                                Map.of(
                                                                        "status",
                                                                        "ok",
                                                                        "value",
                                                                        Map.of(
                                                                                "$ferricstore_bytes",
                                                                                "not-base64!"))))));
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            HttpTransportException error =
                    assertThrows(
                            HttpTransportException.class, () -> executor.execute(List.of("PING")));

            assertEquals("invalid_response", error.errorCode());
            assertTrue(error.getMessage().contains("malformed binary value"));
        }
    }

    @Test
    void basicAuthenticationMatchesTheHttpServerContract() throws IOException {
        List<String> authorizations = new CopyOnWriteArrayList<>();
        try (TestServer server =
                server(
                        exchange -> {
                            authorizations.add(
                                    exchange.getRequestHeaders().getFirst("Authorization"));
                            replyOk(exchange, "PONG");
                        })) {
            HttpTransportOptions options =
                    HttpTransportOptions.builder()
                            .username("worker")
                            .password("secret:with:colons")
                            .allowInsecureBasicAuthentication(true)
                            .build();
            try (FerricStoreClient client =
                    FerricStoreClient.connect(server.url(), new RawCodec(), options)) {
                assertArrayEquals(bytes("PONG"), (byte[]) client.command("PING"));
            }
        }

        String expected =
                "Basic " + Base64.getEncoder().encodeToString(bytes("worker:secret:with:colons"));
        assertEquals(List.of(expected), authorizations);
    }

    @Test
    void rejectsBasicCredentialsOverCleartextUnlessExplicitlyAllowed() {
        HttpTransportOptions options =
                HttpTransportOptions.builder().username("worker").password("secret").build();

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> HttpExecutor.connect("http://127.0.0.1:8080", options));

        assertTrue(error.getMessage().contains("allowInsecureBasicAuthentication"));
    }

    @Test
    void preservesAuthorizationAcrossRedirectOriginsAsDeploymentPolicy() throws IOException {
        List<String> targetAuthorizations = new CopyOnWriteArrayList<>();
        try (TestServer target =
                        server(
                                exchange -> {
                                    targetAuthorizations.add(
                                            exchange.getRequestHeaders().getFirst("Authorization"));
                                    replyOk(exchange, "redirected");
                                });
                TestServer redirect =
                        server(
                                exchange -> {
                                    exchange.getResponseHeaders()
                                            .set("Location", target.url() + "/v1/commands");
                                    exchange.sendResponseHeaders(307, -1);
                                    exchange.close();
                                })) {
            HttpTransportOptions options =
                    HttpTransportOptions.builder().bearerToken("secret").build();
            try (FerricStoreClient client =
                    FerricStoreClient.connect(redirect.url(), new RawCodec(), options)) {
                assertArrayEquals(bytes("redirected"), (byte[]) client.command("PING"));
            }
        }

        assertEquals(List.of("Bearer secret"), targetAuthorizations);
    }

    @Test
    void failsAfterTenFollowedRedirects() throws IOException {
        AtomicInteger requests = new AtomicInteger();
        try (TestServer server =
                        server(
                                exchange -> {
                                    requests.incrementAndGet();
                                    exchange.getResponseHeaders().set("Location", "/v1/commands");
                                    exchange.sendResponseHeaders(307, -1);
                                    exchange.close();
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            HttpTransportException error =
                    assertThrows(
                            HttpTransportException.class, () -> executor.execute(List.of("PING")));

            assertEquals("transport_error", error.errorCode());
            assertTrue(error.getCause().getMessage().contains("too many HTTP redirects"));
            assertEquals(11, requests.get());
        }
    }

    @Test
    void rejectsConnectionAffineCommandsBeforeSendingARequest() throws IOException {
        AtomicInteger requests = new AtomicInteger();
        try (TestServer server =
                        server(
                                exchange -> {
                                    requests.incrementAndGet();
                                    replyOk(exchange, "unexpected");
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            for (String command :
                    List.of(
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
                            "WINDOW_UPDATE")) {
                IllegalArgumentException error =
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> executor.execute(List.of(command)));
                assertTrue(error.getMessage().contains("native TCP"));
            }
            for (String command : List.of("AUTH", "SUBSCRIBE", "WATCH")) {
                IllegalArgumentException error =
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> executor.execute(List.of("COMMAND_EXEC", command)));
                assertTrue(error.getMessage().contains("native TCP"));
            }
            assertEquals(0, requests.get());
        }
    }

    @Test
    void supportsBlockingCommandsAndExtendsTheirRequestDeadline() throws IOException {
        List<String> commands = new CopyOnWriteArrayList<>();
        try (TestServer server =
                        server(
                                exchange -> {
                                    Object encoded =
                                            ((List<?>) readJson(exchange).get("commands")).get(0);
                                    commands.add(
                                            encoded instanceof List<?> command
                                                    ? command.get(0).toString()
                                                    : ((Map<?, ?>) encoded)
                                                            .get("command")
                                                            .toString());
                                    try {
                                        Thread.sleep(75);
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                    }
                                    replyOk(exchange, "ready");
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofMillis(25))
                                        .build())) {
            for (List<Object> command :
                    List.of(
                            List.<Object>of("BLPOP", "queue", 1),
                            List.<Object>of("BLMPOP", 1, 1, "queue", "LEFT"),
                            List.<Object>of("BZPOPMIN", "scores", 1),
                            List.<Object>of("BZMPOP", 1, 1, "scores", "MIN"),
                            List.<Object>of("XREAD", "BLOCK", 0, "STREAMS", "events", "$"),
                            List.<Object>of(
                                    "XREADGROUP",
                                    "GROUP",
                                    "workers",
                                    "worker-1",
                                    "BLOCK",
                                    200,
                                    "STREAMS",
                                    "events",
                                    ">"),
                            List.<Object>of("WAIT", 1, 200),
                            List.<Object>of("WAITAOF", 1, 1, 200),
                            List.<Object>of(
                                    "FLOW.CLAIM_DUE", "jobs", "WORKER", "worker-1", "BLOCK", 200),
                            List.<Object>of("FLOW.SCHEDULE.FIRE_DUE", "BLOCK", 200))) {
                assertArrayEquals(bytes("ready"), (byte[]) executor.execute(command));
            }
        }
        assertEquals(
                List.of(
                        "BLPOP",
                        "BLMPOP",
                        "BZPOPMIN",
                        "BZMPOP",
                        "XREAD",
                        "XREADGROUP",
                        "WAIT",
                        "WAITAOF",
                        "FLOW.CLAIM_DUE",
                        "FLOW.SCHEDULE.FIRE_DUE"),
                commands);
    }

    @Test
    void flowClaimDueBlockZeroDisablesTheDefaultRequestDeadline() throws IOException {
        try (TestServer server =
                        server(
                                exchange -> {
                                    try {
                                        Thread.sleep(75);
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                    }
                                    replyOk(exchange, "ready");
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(
                                server.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofMillis(25))
                                        .build())) {
            assertArrayEquals(
                    bytes("ready"),
                    (byte[])
                            executor.execute(
                                    List.of(
                                            "FLOW.CLAIM_DUE",
                                            "jobs",
                                            "WORKER",
                                            "worker-1",
                                            "BLOCK",
                                            0)));
        }
    }

    @Test
    void mapsTopLevelAndPerCommandErrorsWithoutUnsafeRetries() throws IOException {
        AtomicInteger response = new AtomicInteger();
        try (TestServer server =
                        server(
                                exchange -> {
                                    if (response.getAndIncrement() == 0) {
                                        replyJson(
                                                exchange,
                                                401,
                                                Map.of("error", Map.of("code", "unauthenticated")));
                                        return;
                                    }
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of(
                                                                    "status",
                                                                    "error",
                                                                    "error",
                                                                    Map.of(
                                                                            "code",
                                                                            "noperm",
                                                                            "message",
                                                                            "denied",
                                                                            "retryable",
                                                                            false,
                                                                            "safe_to_retry",
                                                                            false)))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            HttpTransportException topLevel =
                    assertThrows(
                            HttpTransportException.class, () -> executor.execute(List.of("PING")));
            assertEquals(401, topLevel.statusCode());
            assertEquals("unauthenticated", topLevel.errorCode());
            assertFalse(topLevel.safeToRetry());

            HttpCommandException command =
                    assertThrows(
                            HttpCommandException.class,
                            () -> executor.execute(List.of("GET", "protected")));
            assertEquals("noperm", command.errorCode());
            assertFalse(command.retryable());
            assertFalse(command.safeToRetry());
        }
    }

    @Test
    void httpExceptionsRemainSerializableWhenRawGatewayDetailsAreNot() throws Exception {
        HttpCommandException command =
                new HttpCommandException(
                        "denied",
                        "noperm",
                        false,
                        false,
                        100L,
                        Map.of("non_serializable", new Object()));
        HttpTransportException transport =
                new HttpTransportException(
                        "unavailable",
                        503,
                        "server_overloaded",
                        true,
                        true,
                        250L,
                        Map.of("non_serializable", new Object()),
                        null);

        HttpCommandException restoredCommand = roundTrip(command);
        HttpTransportException restoredTransport = roundTrip(transport);

        assertEquals("noperm", restoredCommand.errorCode());
        assertEquals(100L, restoredCommand.retryAfterMs());
        assertTrue(restoredCommand.raw().isEmpty());
        assertEquals(503, restoredTransport.statusCode());
        assertEquals("server_overloaded", restoredTransport.errorCode());
        assertEquals(250L, restoredTransport.retryAfterMs());
        assertTrue(restoredTransport.raw().isEmpty());
    }

    @Test
    void encodesStructuredOnlyFlowCommandsForTheSharedHttpGateway() throws IOException {
        List<Map<String, Object>> requests = new CopyOnWriteArrayList<>();
        try (TestServer server =
                        server(
                                exchange -> {
                                    requests.add(readJson(exchange));
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of(
                                                                    "status",
                                                                    "ok",
                                                                    "value",
                                                                    List.of("one", "two")))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            assertEquals(
                    List.of("one", "two"),
                    executor.execute(
                            List.of("FLOW.VALUE.MGET", "ref-a", "ref-b", "MAX_BYTES", 1024L)));
        }

        Object rawCommand = ((List<?>) requests.get(0).get("commands")).get(0);
        Map<String, Object> command = Resp.map(rawCommand);
        assertEquals("FLOW.VALUE.MGET", command.get("command"));
        assertEquals(0x020C, command.get("opcode"));
        @SuppressWarnings("unchecked")
        Map<String, Object> encodedPayload = (Map<String, Object>) command.get("payload");
        @SuppressWarnings("unchecked")
        List<List<Object>> pairs = (List<List<Object>>) encodedPayload.get("$ferricstore_map");
        assertTrue(pairs.stream().anyMatch(pair -> pair.equals(List.of("max_bytes", 1024))));
    }

    @Test
    void compactsHomogeneousFlowCreatesIntoOneStructuredCreateManyCommand() throws Exception {
        AtomicReference<Map<String, Object>> request = new AtomicReference<>();
        try (TestServer server =
                        server(
                                exchange -> {
                                    request.set(readJson(exchange));
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of(
                                                                    "status", "ok", "value",
                                                                    "OK"))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            List<Object> results =
                    executor.pipelineAsync(
                                    List.of(
                                            flowCreate("id-a", null),
                                            flowCreate("id-b", new byte[] {0, (byte) 0xff})))
                            .get(5, TimeUnit.SECONDS);
            assertEquals(List.of("OK", "OK"), results);
        }

        List<?> commands = (List<?>) request.get().get("commands");
        assertEquals(1, commands.size());
        Map<String, Object> descriptor = Resp.map(commands.get(0));
        assertEquals("FLOW.CREATE_MANY", descriptor.get("command"));
        assertEquals(0x020F, descriptor.get("opcode"));
        Map<String, Object> payload = decodeBinaryMap(descriptor.get("payload"));
        assertEquals(Boolean.TRUE, payload.get("independent"));
        assertEquals("ok_on_success", payload.get("return"));
        List<?> items = (List<?>) payload.get("items");
        Map<String, Object> first = decodeBinaryMap(items.get(0));
        Map<String, Object> second = decodeBinaryMap(items.get(1));
        assertFalse(first.containsKey("payload"));
        assertArrayEquals(new byte[] {0, (byte) 0xff}, decodeBytes(second.get("payload")));
    }

    @Test
    void keepsIncompatibleFlowCreatesAsOrdinaryHttpPipelineCommands() throws Exception {
        AtomicReference<Map<String, Object>> request = new AtomicReference<>();
        try (TestServer server =
                        server(
                                exchange -> {
                                    request.set(readJson(exchange));
                                    replyJson(
                                            exchange,
                                            200,
                                            Map.of(
                                                    "encoding",
                                                    "ferricstore-json-v1",
                                                    "results",
                                                    List.of(
                                                            Map.of("status", "ok", "value", "OK"),
                                                            Map.of(
                                                                    "status", "ok", "value",
                                                                    "OK"))));
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(server.url(), HttpTransportOptions.defaults())) {
            List<Object> second = new ArrayList<>(flowCreate("id-b", null));
            second.set(second.indexOf(123L), 124L);
            assertEquals(
                    List.of("OK", "OK"),
                    executor.pipelineAsync(List.of(flowCreate("id-a", null), second))
                            .get(5, TimeUnit.SECONDS));
        }

        assertEquals(2, ((List<?>) request.get().get("commands")).size());
    }

    @Test
    void enforcesAbsoluteTimeoutAndResponseLimit() throws IOException {
        try (TestServer slow =
                        server(
                                exchange -> {
                                    try {
                                        Thread.sleep(250);
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                    }
                                    replyOk(exchange, "late");
                                });
                HttpExecutor executor =
                        HttpExecutor.connect(
                                slow.url(),
                                HttpTransportOptions.builder()
                                        .requestTimeout(Duration.ofMillis(50))
                                        .build())) {
            HttpTransportException timeout =
                    assertThrows(
                            HttpTransportException.class, () -> executor.execute(List.of("PING")));
            assertEquals("transport_timeout", timeout.errorCode());
            assertFalse(timeout.safeToRetry());
        }

        try (TestServer large = server(exchange -> replyOk(exchange, "x".repeat(1_024)));
                HttpExecutor executor =
                        HttpExecutor.connect(
                                large.url(),
                                HttpTransportOptions.builder().maxResponseBytes(128).build())) {
            HttpTransportException tooLarge =
                    assertThrows(
                            HttpTransportException.class, () -> executor.execute(List.of("PING")));
            assertEquals("response_too_large", tooLarge.errorCode());
        }
    }

    @Test
    void closeAndInvalidSchemesFailClearly() throws IOException {
        try (TestServer server = server(exchange -> replyOk(exchange, "PONG"))) {
            HttpExecutor executor =
                    HttpExecutor.connect(server.url(), HttpTransportOptions.defaults());
            executor.close();
            assertThrows(IllegalStateException.class, () -> executor.execute(List.of("PING")));
        }

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> FerricStoreClient.connect("redis://127.0.0.1:6379"));
        assertTrue(error.getMessage().contains("ferric://, ferrics://, http://, or https://"));
    }

    private static TestServer capacityServer(
            CountDownLatch firstArrived, CountDownLatch release, AtomicInteger requests)
            throws IOException {
        return server(
                exchange -> {
                    readJson(exchange);
                    int request = requests.incrementAndGet();
                    if (request == 1) {
                        firstArrived.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new IOException("held request was not released");
                            }
                        } catch (InterruptedException error) {
                            Thread.currentThread().interrupt();
                            throw new IOException("HTTP test server interrupted", error);
                        }
                    }
                    replyOk(exchange, "reply-" + request);
                });
    }

    private static TestServer server(Handler handler) throws IOException {
        HttpServer server =
                HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        handler.handle(exchange);
                    } catch (Exception error) {
                        byte[] body = error.toString().getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(500, body.length);
                        exchange.getResponseBody().write(body);
                        exchange.close();
                    }
                });
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
        return new TestServer(server, executor);
    }

    private static Map<String, Object> readJson(HttpExchange exchange) throws IOException {
        return JSON.readValue(exchange.getRequestBody(), new TypeReference<>() {});
    }

    private static void replyOk(HttpExchange exchange, String value) throws IOException {
        replyJson(
                exchange,
                200,
                Map.of(
                        "encoding",
                        "ferricstore-json-v1",
                        "results",
                        List.of(Map.of("status", "ok", "value", bytesMarker(bytes(value))))));
    }

    private static void replyJson(HttpExchange exchange, int status, Object value)
            throws IOException {
        byte[] body = JSON.writeValueAsBytes(value);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static Map<String, String> bytesMarker(byte[] value) {
        return Map.of("$ferricstore_bytes", Base64.getEncoder().encodeToString(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static List<Object> flowCreate(String id, byte[] payload) {
        List<Object> command =
                new ArrayList<>(
                        List.of("FLOW.CREATE", id, "TYPE", "type", "STATE", "queued", "NOW", 123L));
        if (payload != null) {
            command.add("PAYLOAD");
            command.add(payload);
        }
        command.add("RUN_AT");
        command.add(123L);
        command.add("PRIORITY");
        command.add(0);
        return command;
    }

    private static Map<String, Object> decodeBinaryMap(Object value) {
        Map<String, Object> marker = Resp.map(value);
        Object pairs = marker.get("$ferricstore_map");
        Map<String, Object> decoded = new java.util.LinkedHashMap<>();
        for (Object entry : (List<?>) pairs) {
            List<?> pair = (List<?>) entry;
            decoded.put(String.valueOf(pair.get(0)), pair.get(1));
        }
        return decoded;
    }

    private static byte[] decodeBytes(Object value) {
        return Base64.getDecoder()
                .decode(String.valueOf(Resp.map(value).get("$ferricstore_bytes")));
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private static final class TestServer implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;

        private TestServer(HttpServer server, ExecutorService executor) {
            this.server = server;
            this.executor = executor;
        }

        private String url() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
