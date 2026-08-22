package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

final class NativeExecutorTest {
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
