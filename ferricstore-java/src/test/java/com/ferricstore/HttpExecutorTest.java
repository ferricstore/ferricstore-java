package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class HttpExecutorTest {
    private static final ObjectMapper JSON = new ObjectMapper();

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
                            "AUTH",
                            "BLMOVE",
                            "BLMPOP",
                            "BLPOP",
                            "BRPOP",
                            "CLIENT",
                            "DISCARD",
                            "EXEC",
                            "FETCH_OR_COMPUTE",
                            "FETCH_OR_COMPUTE_ERROR",
                            "FETCH_OR_COMPUTE_RESULT",
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
                            "WATCH",
                            "XREAD",
                            "XREADGROUP")) {
                IllegalArgumentException error =
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> executor.execute(List.of(command)));
                assertTrue(error.getMessage().contains("native TCP"));
            }
            assertEquals(0, requests.get());
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
