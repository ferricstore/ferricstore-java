package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class NativeExecutorTest {
    @Test
    void batchesPayloadlessFlowCreatesWithoutInventingAnEmptyPayload() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    assertEquals(
                                            List.of("kv_mget_v1", "ok_list_v1", "pipeline_v1"),
                                            list(map(hello.body()).get("compact_response_codecs"))
                                                    .stream()
                                                    .map(NativeExecutorTest::text)
                                                    .toList());
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame createMany = readRequest(socket);
                                    assertEquals(0x020F, createMany.identity().opcode());
                                    assertEquals(0, createMany.flags());
                                    Map<String, Object> payload = map(createMany.body());
                                    List<Object> items = list(payload.get("items"));
                                    assertEquals(2, items.size());
                                    assertFalse(objectMap(items.get(0)).containsKey("payload"));
                                    assertFalse(objectMap(items.get(1)).containsKey("payload"));
                                    writeResponse(
                                            socket,
                                            createMany.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "OK");
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                List<Object> results =
                        executor.pipelineAsync(List.of(flowCreate("id-a"), flowCreate("id-b")))
                                .get(5, TimeUnit.SECONDS);
                assertEquals(
                        List.of("OK", "OK"),
                        results.stream().map(NativeExecutorTest::text).toList());
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void batchesFlowCreatesWithPayloadAsOneCompactNativeRequest() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame createMany = readRequest(socket);
                                    assertEquals(0x020F, createMany.identity().opcode());
                                    assertEquals(
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD, createMany.flags());
                                    assertEquals(0x90, Byte.toUnsignedInt(createMany.body()[0]));
                                    writeResponse(
                                            socket,
                                            createMany.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "OK");
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                List<Object> results =
                        executor.pipelineAsync(
                                        List.of(
                                                flowCreate("id-a", new byte[] {0, 1}),
                                                flowCreate("id-b", new byte[] {(byte) 0xff})))
                                .get(5, TimeUnit.SECONDS);
                assertEquals(
                        List.of("OK", "OK"),
                        results.stream().map(NativeExecutorTest::text).toList());
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void compactGetPipelinePreservesMissingValuesWithoutPerItemFrames() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame pipeline = readRequest(socket);
                                    assertEquals(
                                            NativeProtocol.OP_PIPELINE,
                                            pipeline.identity().opcode());
                                    assertEquals(
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD, pipeline.flags());
                                    assertEquals(0x94, Byte.toUnsignedInt(pipeline.body()[0]));
                                    assertEquals(2, Byte.toUnsignedInt(pipeline.body()[1]));
                                    writeRawResponse(
                                            socket,
                                            pipeline.identity(),
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD,
                                            compactPipelineBody(new byte[] {1, 2}, null));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                List<Object> results =
                        executor.pipelineAsync(
                                        List.of(List.of("GET", "key-a"), List.of("GET", "missing")))
                                .get(5, TimeUnit.SECONDS);
                assertArrayEquals(new byte[] {1, 2}, (byte[]) results.get(0));
                assertEquals(null, results.get(1));
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void sendsAnAsyncPipelineAsOneNativePipelineRequestAndPreservesOrder() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame pipeline = readRequest(socket);
                                    assertEquals(
                                            NativeProtocol.OP_PIPELINE,
                                            pipeline.identity().opcode());
                                    Map<String, Object> payload = map(pipeline.body());
                                    assertEquals("none", text(payload.get("atomicity")));
                                    assertEquals("pairs", text(payload.get("return")));
                                    List<Object> commands = list(payload.get("commands"));
                                    assertEquals(2, commands.size());
                                    Map<String, Object> first = objectMap(commands.get(0));
                                    Map<String, Object> second = objectMap(commands.get(1));
                                    assertEquals(
                                            (long) NativeProtocol.OP_COMMAND_EXEC,
                                            first.get("opcode"));
                                    assertEquals(1L, first.get("request_id"));
                                    assertEquals(
                                            "GET",
                                            text(objectMap(first.get("body")).get("command")));
                                    assertEquals(2L, second.get("request_id"));
                                    assertEquals(
                                            "SET",
                                            text(objectMap(second.get("body")).get("command")));

                                    writeResponse(
                                            socket,
                                            pipeline.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            List.of(
                                                    List.of("ok", "first"),
                                                    List.of("ok", "second")));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                assertEquals(
                        List.of("first", "second"),
                        executor
                                .pipelineAsync(
                                        List.of(
                                                List.of("GET", "key-a"),
                                                List.of("SET", "key-b", "value")))
                                .get(5, TimeUnit.SECONDS)
                                .stream()
                                .map(NativeExecutorTest::text)
                                .toList());
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void multiplexesConcurrentRequestsAndCorrelatesOutOfOrderResponses() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame first = readRequest(socket);
                                    NativeFrame second = readRequest(socket);
                                    String firstValue = commandArgument(first);
                                    String secondValue = commandArgument(second);
                                    writeResponse(
                                            socket,
                                            second.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            secondValue);
                                    writeResponse(
                                            socket,
                                            first.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            firstValue);
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                CompletableFuture<Object> alpha = executor.executeAsync(List.of("ECHO", "alpha"));
                CompletableFuture<Object> beta = executor.executeAsync(List.of("ECHO", "beta"));

                assertFalse(alpha.isDone());
                assertFalse(beta.isDone());
                assertEquals("alpha", text(alpha.get()));
                assertEquals("beta", text(beta.get()));
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void rejectsRequestsBeyondTheBoundedNativePendingSet() throws Exception {
        CountDownLatch firstArrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame first = readRequest(socket);
                                    firstArrived.countDown();
                                    if (!release.await(5, TimeUnit.SECONDS)) {
                                        throw new IOException(
                                                "held native request was not released");
                                    }
                                    writeResponse(
                                            socket,
                                            first.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "first");
                                }
                                return null;
                            });

            NativeTransportOptions options =
                    NativeTransportOptions.builder().maxPendingRequests(1).build();
            try (NativeExecutor executor =
                    NativeExecutor.connectWithOptions(
                            "ferric://127.0.0.1:" + server.getLocalPort(), options)) {
                CompletableFuture<Object> held = executor.executeAsync(List.of("PING"));
                assertTrue(firstArrived.await(5, TimeUnit.SECONDS));
                CompletableFuture<Object> overflow = executor.executeAsync(List.of("PING"));

                ExecutionException failure =
                        assertThrows(
                                ExecutionException.class, () -> overflow.get(1, TimeUnit.SECONDS));
                NativeProtocolException overloaded =
                        assertInstanceOf(NativeProtocolException.class, failure.getCause());
                assertTrue(overloaded.getMessage().contains("pending request limit"));

                release.countDown();
                assertEquals("first", text(held.get(5, TimeUnit.SECONDS)));
            } finally {
                release.countDown();
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void closeFailsAnAsyncRequestThatIsAlreadyInFlight() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));
                                    readRequest(socket);
                                    received.countDown();
                                    socket.getInputStream().read();
                                }
                                return null;
                            });

            NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort());
            CompletableFuture<Object> pending =
                    executor.executeAsync(List.of("SET", "key", "value"));
            assertTrue(received.await(5, TimeUnit.SECONDS));
            executor.close();

            ExecutionException failure =
                    assertThrows(ExecutionException.class, () -> pending.get(1, TimeUnit.SECONDS));
            assertInstanceOf(NativeProtocolException.class, failure.getCause());
            assertTrue(failure.getCause().getMessage().contains("requests in flight"));
            served.get(1, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void cancelledRequestDoesNotPoisonTheMultiplexedConnection() throws Exception {
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch respond = new CountDownLatch(1);
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame cancelled = readRequest(socket);
                                    firstReceived.countDown();
                                    assertTrue(respond.await(5, TimeUnit.SECONDS));
                                    writeResponse(
                                            socket,
                                            cancelled.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "ignored");

                                    NativeFrame next = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            next.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            commandArgument(next));
                                }
                                return null;
                            });

            NativeTransportOptions options =
                    NativeTransportOptions.builder().maxPendingRequests(1).build();
            try (NativeExecutor executor =
                    NativeExecutor.connectWithOptions(
                            "ferric://127.0.0.1:" + server.getLocalPort(), options)) {
                CompletableFuture<Object> cancelled =
                        executor.executeAsync(List.of("ECHO", "cancelled"));
                assertTrue(firstReceived.await(5, TimeUnit.SECONDS));
                assertTrue(cancelled.cancel(false));
                respond.countDown();

                assertEquals(
                        "still-usable",
                        text(
                                executor.executeAsync(List.of("ECHO", "still-usable"))
                                        .get(5, TimeUnit.SECONDS)));
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            respond.countDown();
            tasks.shutdownNow();
        }
    }

    @Test
    void negotiatesAuthenticatesBeforeDataAndReassemblesChunkedResponses() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    assertEquals(
                                            new NativeFrame.Identity(
                                                    0,
                                                    NativeProtocol.OP_HELLO,
                                                    hello.identity().requestId()),
                                            hello.identity());
                                    assertEquals(
                                            "none", text(map(hello.body()).get("compression")));
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(true, 4096));

                                    NativeFrame auth = readRequest(socket);
                                    assertEquals(NativeProtocol.OP_AUTH, auth.identity().opcode());
                                    assertEquals(0, auth.identity().laneId());
                                    Map<String, Object> credentials = map(auth.body());
                                    assertEquals("sdk-user", text(credentials.get("username")));
                                    assertEquals("s:ecret", text(credentials.get("password")));
                                    writeResponse(
                                            socket,
                                            auth.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "OK");

                                    NativeFrame command = readRequest(socket);
                                    assertEquals(
                                            NativeProtocol.OP_COMMAND_EXEC,
                                            command.identity().opcode());
                                    assertTrue(command.identity().laneId() > 0);
                                    Map<String, Object> payload = map(command.body());
                                    assertEquals("GET", text(payload.get("command")));
                                    assertEquals("key", text(list(payload.get("args")).get(0)));

                                    byte[] body =
                                            responseBody(
                                                    NativeProtocol.STATUS_OK, new byte[] {1, 2, 3});
                                    int split = 3;
                                    writeRawResponse(
                                            socket,
                                            command.identity(),
                                            NativeProtocol.FLAG_MORE_CHUNKS,
                                            java.util.Arrays.copyOfRange(body, 0, split));
                                    writeRawResponse(
                                            socket,
                                            command.identity(),
                                            0,
                                            java.util.Arrays.copyOfRange(body, split, body.length));
                                }
                                return null;
                            });

            String password = URLEncoder.encode("s:ecret", StandardCharsets.UTF_8);
            try (NativeExecutor executor =
                    NativeExecutor.connect(
                            "ferric://sdk-user:"
                                    + password
                                    + "@127.0.0.1:"
                                    + server.getLocalPort())) {
                assertArrayEquals(
                        new byte[] {1, 2, 3}, (byte[]) executor.execute(List.of("GET", "key")));
                assertEquals(4096, executor.negotiatedCapabilities().maxResponseBytes());
                assertEquals(
                        "kv_mget_v1",
                        executor.negotiatedCapabilities().compactResponseCodecs().get(0x0104));
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void sendsFlowQueriesThroughTheDedicatedTypedOpcode() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame query = readRequest(socket);
                                    assertEquals(
                                            NativeProtocol.OP_FLOW_QUERY,
                                            query.identity().opcode());
                                    assertTrue(query.identity().laneId() > 0);
                                    Map<String, Object> payload = map(query.body());
                                    assertEquals("FQL1", text(payload.get("version")));
                                    assertEquals(
                                            "FROM runs WHERE partition_key = @partition RETURN COUNT",
                                            text(payload.get("query")));
                                    Map<String, Object> params = objectMap(payload.get("params"));
                                    assertEquals("tenant-a", text(params.get("partition")));
                                    assertEquals(7L, params.get("minimum"));
                                    writeResponse(
                                            socket,
                                            query.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            Map.of(
                                                    "version",
                                                    "ferric.flow.query.result/v1",
                                                    "result",
                                                    Map.of("kind", "count", "value", 1L)));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                Object response =
                        executor.flowQuery(
                                "FROM runs WHERE partition_key = @partition RETURN COUNT",
                                Map.of("partition", "tenant-a", "minimum", 7L));
                assertEquals(1L, objectMap(objectMap(response).get("result")).get("value"));
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void sendsStructuredOnlyFlowCommandsThroughTheirCataloguedOpcode() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame mget = readRequest(socket);
                                    assertEquals(0x020C, mget.identity().opcode());
                                    Map<String, Object> payload = map(mget.body());
                                    assertEquals(
                                            List.of("ref-a", "ref-b", "ref-missing"),
                                            list(payload.get("refs")).stream()
                                                    .map(NativeExecutorTest::text)
                                                    .toList());
                                    assertEquals(1024L, payload.get("max_bytes"));
                                    writeRawResponse(
                                            socket,
                                            mget.identity(),
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD,
                                            compactMgetBody("one", null, "two"));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                assertEquals(
                        java.util.Arrays.asList("one", null, "two"),
                        list(
                                        executor.execute(
                                                List.of(
                                                        "FLOW.VALUE.MGET",
                                                        "ref-a",
                                                        "ref-b",
                                                        "ref-missing",
                                                        "MAX_BYTES",
                                                        1024L)))
                                .stream()
                                .map(value -> value == null ? null : text(value))
                                .toList());
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void sendsCompatibleFlowManyMutationsWithTheirCompactTypedOpcodes() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));

                                    NativeFrame transition = readRequest(socket);
                                    assertEquals(0x0211, transition.identity().opcode());
                                    assertEquals(
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD, transition.flags());
                                    assertEquals(0x9C, Byte.toUnsignedInt(transition.body()[0]));
                                    writeResponse(
                                            socket,
                                            transition.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "OK");

                                    NativeFrame complete = readRequest(socket);
                                    assertEquals(0x0210, complete.identity().opcode());
                                    assertEquals(
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD, complete.flags());
                                    assertEquals(0x93, Byte.toUnsignedInt(complete.body()[0]));
                                    writeRawResponse(
                                            socket,
                                            complete.identity(),
                                            NativeProtocol.FLAG_CUSTOM_PAYLOAD,
                                            compactOkListBody(1));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                assertEquals(
                        "OK",
                        text(
                                executor.execute(
                                        List.of(
                                                "FLOW.TRANSITION_MANY",
                                                "MIXED",
                                                "running",
                                                "done",
                                                "NOW",
                                                100L,
                                                "INDEPENDENT",
                                                true,
                                                "RETURN",
                                                "OK_ON_SUCCESS",
                                                "ITEMS",
                                                "a",
                                                "p1",
                                                1L,
                                                "lease-a"))));
                List<Object> completed =
                        list(
                                executor.execute(
                                        List.of(
                                                "FLOW.COMPLETE_MANY",
                                                "MIXED",
                                                "NOW",
                                                100L,
                                                "INDEPENDENT",
                                                true,
                                                "RETURN",
                                                "OK_ON_SUCCESS",
                                                "ITEMS",
                                                "a",
                                                "p1",
                                                "lease-a",
                                                1L)));
                assertEquals("OK", text(completed.get(0)));
            }
            served.get(5, TimeUnit.SECONDS);
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void rejectsMissing08HelloCapabilitiesWithoutSendingADataCommand() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            Map.of(
                                                    "protocol",
                                                    "ferricstore-native",
                                                    "version",
                                                    1L,
                                                    "capabilities",
                                                    Map.of(
                                                            "limits",
                                                            Map.of("max_response_bytes", 4096L))));
                                }
                                return null;
                            });

            NativeProtocolException error =
                    assertThrows(
                            NativeProtocolException.class,
                            () ->
                                    NativeExecutor.connect(
                                            "ferric://127.0.0.1:" + server.getLocalPort()));
            assertTrue(error.getMessage().contains("minimum 0.8.0 HELLO contract"));
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void appliesNegotiatedResponseLimitBeforeAllocatingTheNextFrame() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 32));
                                    NativeFrame command = readRequest(socket);
                                    writeResponseHeader(socket, command.identity(), 0, 33);
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                NativeProtocolException error =
                        assertThrows(
                                NativeProtocolException.class,
                                () -> executor.execute(List.of("GET", "key")));
                assertTrue(error.getMessage().contains("max_response_bytes"), error::getMessage);
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void retriesOnlyWhenTheServerMarksBusyOrRerouteRetryableAndSafe() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Void> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));
                                    NativeFrame first = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            first.identity(),
                                            0,
                                            NativeProtocol.STATUS_BUSY,
                                            Map.of(
                                                    "message",
                                                    "busy",
                                                    "retryable",
                                                    true,
                                                    "safe_to_retry",
                                                    true,
                                                    "retry_after_ms",
                                                    0L));
                                    NativeFrame retried = readRequest(socket);
                                    Map<String, Object> firstPayload = map(first.body());
                                    Map<String, Object> retriedPayload = map(retried.body());
                                    assertEquals(
                                            text(firstPayload.get("command")),
                                            text(retriedPayload.get("command")));
                                    assertEquals(
                                            text(list(firstPayload.get("args")).get(0)),
                                            text(list(retriedPayload.get("args")).get(0)));
                                    writeResponse(
                                            socket,
                                            retried.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            "OK");

                                    NativeFrame unsafe = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            unsafe.identity(),
                                            0,
                                            NativeProtocol.STATUS_REROUTE,
                                            Map.of(
                                                    "message",
                                                    "unsafe",
                                                    "retryable",
                                                    true,
                                                    "safe_to_retry",
                                                    false,
                                                    "retry_after_ms",
                                                    0L));
                                }
                                return null;
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                assertEquals("OK", text(executor.execute(List.of("PING.DATA", "key"))));
                NativeServerException error =
                        assertThrows(
                                NativeServerException.class,
                                () -> executor.execute(List.of("SET", "key", "value")));
                assertEquals(true, error.retryable());
                assertEquals(false, error.safeToRetry());
            }
            served.get();
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void neverReplaysACommandAfterTheConnectionDropsWithAnUnknownOutcome() throws Exception {
        ExecutorService tasks = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            Future<Integer> served =
                    tasks.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    NativeFrame hello = readRequest(socket);
                                    writeResponse(
                                            socket,
                                            hello.identity(),
                                            0,
                                            NativeProtocol.STATUS_OK,
                                            hello(false, 4096));
                                    readRequest(socket);
                                    return 1;
                                }
                            });

            try (NativeExecutor executor =
                    NativeExecutor.connect("ferric://127.0.0.1:" + server.getLocalPort())) {
                NativeProtocolException error =
                        assertThrows(
                                NativeProtocolException.class,
                                () -> executor.execute(List.of("SET", "key", "value")));
                assertTrue(error.getMessage().contains("outcome is unknown"));
            }
            assertEquals(1, served.get());
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void requiresFerricNativeUris() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> NativeExecutor.connect("redis://127.0.0.1:6379"));
        assertTrue(error.getMessage().contains("ferric:// or ferrics://"));
    }

    private static Map<String, Object> hello(boolean authRequired, int maxResponseBytes) {
        Map<String, Object> codecs = new LinkedHashMap<>();
        codecs.put("flow_query_result_v1", List.of(0x0100L));
        codecs.put("kv_mget_v1", List.of(0x0104L, 0x020CL));
        codecs.put("ok_list_v1", List.of(0x0102L, 0x0105L, 0x020FL, 0x0210L));
        codecs.put("pipeline_v1", List.of((long) NativeProtocol.OP_PIPELINE));
        return Map.of(
                "protocol",
                "ferricstore-native",
                "version",
                1L,
                "auth_required",
                authRequired,
                "capabilities",
                Map.of(
                        "limits", Map.of("max_response_bytes", (long) maxResponseBytes),
                        "response_codecs", Map.of("compact_response_opcodes", codecs)));
    }

    private static String commandArgument(NativeFrame frame) {
        Map<String, Object> payload = map(frame.body());
        assertEquals("ECHO", text(payload.get("command")));
        return text(list(payload.get("args")).get(0));
    }

    private static List<Object> flowCreate(String id) {
        return List.of(
                "FLOW.CREATE",
                id,
                "TYPE",
                "type",
                "STATE",
                "queued",
                "NOW",
                123L,
                "RUN_AT",
                123L,
                "PRIORITY",
                0);
    }

    private static List<Object> flowCreate(String id, byte[] payload) {
        return List.of(
                "FLOW.CREATE",
                id,
                "TYPE",
                "type",
                "STATE",
                "queued",
                "NOW",
                123L,
                "PAYLOAD",
                payload,
                "RUN_AT",
                123L,
                "PRIORITY",
                0);
    }

    private static NativeFrame readRequest(Socket socket) throws IOException {
        byte[] header = socket.getInputStream().readNBytes(NativeProtocol.HEADER_BYTES);
        assertEquals(NativeProtocol.HEADER_BYTES, header.length);
        ByteBuffer input = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = new byte[NativeProtocol.MAGIC.length];
        input.get(magic);
        assertArrayEquals(NativeProtocol.MAGIC, magic);
        assertEquals(NativeProtocol.REQUEST_VERSION, Byte.toUnsignedInt(input.get()));
        int flags = Byte.toUnsignedInt(input.get());
        long laneId = Integer.toUnsignedLong(input.getInt());
        int opcode = Short.toUnsignedInt(input.getShort());
        long requestId = input.getLong();
        int length = input.getInt();
        assertTrue(length >= 0);
        byte[] body = socket.getInputStream().readNBytes(length);
        assertEquals(length, body.length);
        return new NativeFrame(new NativeFrame.Identity(laneId, opcode, requestId), flags, body);
    }

    private static void writeResponse(
            Socket socket, NativeFrame.Identity identity, int flags, int status, Object value)
            throws IOException {
        writeRawResponse(socket, identity, flags, responseBody(status, value));
    }

    private static void writeRawResponse(
            Socket socket, NativeFrame.Identity identity, int flags, byte[] body)
            throws IOException {
        writeResponseHeader(socket, identity, flags, body.length);
        socket.getOutputStream().write(body);
        socket.getOutputStream().flush();
    }

    private static void writeResponseHeader(
            Socket socket, NativeFrame.Identity identity, int flags, int bodyLength)
            throws IOException {
        ByteBuffer header =
                ByteBuffer.allocate(NativeProtocol.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.put(NativeProtocol.MAGIC);
        header.put((byte) NativeProtocol.RESPONSE_VERSION);
        header.put((byte) flags);
        header.putInt((int) identity.laneId());
        header.putShort((short) identity.opcode());
        header.putLong(identity.requestId());
        header.putInt(bodyLength);
        socket.getOutputStream().write(header.array());
        socket.getOutputStream().flush();
    }

    private static byte[] responseBody(int status, Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(status);
            output.write(NativeValueCodec.encode(value, 64 * 1024));
        }
        return bytes.toByteArray();
    }

    private static byte[] compactMgetBody(String... values) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x83);
            output.writeInt(values.length);
            for (String value : values) {
                if (value == null) {
                    output.writeByte(0);
                } else {
                    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
                    output.writeByte(1);
                    output.writeInt(encoded.length);
                    output.write(encoded);
                }
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] compactPipelineBody(byte[]... values) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x95);
            output.writeInt(values.length);
            for (byte[] value : values) {
                output.writeByte(0);
                if (value == null) {
                    output.writeByte(0);
                } else {
                    output.writeByte(1);
                    output.writeInt(value.length);
                    output.write(value);
                }
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] compactOkListBody(int count) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(NativeProtocol.STATUS_OK);
            output.writeByte(0x81);
            output.writeInt(count);
        }
        return bytes.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(byte[] body) {
        return (Map<String, Object>) NativeValueCodec.decode(body);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static String text(Object value) {
        assertInstanceOf(byte[].class, value);
        return new String((byte[]) value, StandardCharsets.UTF_8);
    }
}
