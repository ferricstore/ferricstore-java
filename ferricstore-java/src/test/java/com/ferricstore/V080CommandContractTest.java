package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class V080CommandContractTest {
    @Test
    void fetchOrComputeUsesTheHintAndOwnershipTokenPositionsAndHasNoTokenlessCompletion() {
        byte[] token = bytes("owner-token");
        CapturingExecutor executor =
                new CapturingExecutor(List.of("compute", "refresh", token), "OK", "OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        FetchOrComputeResult result = client.fetchOrCompute("cache", 1_000, "caller");
        assertEquals("refresh", result.computeHint());
        assertSame(token, result.ownershipToken());
        assertTrue(result.shouldCompute());

        assertTrue(client.fetchOrComputeResult("cache", token, "value", 2_000));
        assertTrue(client.fetchOrComputeError("cache", token, "failed"));
        assertCommand(
                List.of("FETCH_OR_COMPUTE_RESULT", "cache", token, bytes("value"), 2_000L),
                executor.calls.get(1));
        assertCommand(
                List.of("FETCH_OR_COMPUTE_ERROR", "cache", token, "failed"),
                executor.calls.get(2));

        assertThrows(
                NoSuchMethodException.class,
                () ->
                        FerricStoreClient.class.getMethod(
                                "fetchOrComputeResult", String.class, Object.class, long.class));
        assertThrows(
                NoSuchMethodException.class,
                () ->
                        FerricStoreClient.class.getMethod(
                                "fetchOrComputeError", String.class, String.class));
        assertThrows(
                IllegalArgumentException.class,
                () -> client.fetchOrComputeError("cache", new byte[0], "failed"));
    }

    @Test
    void fetchOrComputeRejectsLegacyOrMalformedResponseShapes() {
        for (Object response :
                List.of(
                        List.of("compute", "tokenless"),
                        List.of("hit", bytes("value"), "extra"),
                        List.of("unknown", "value"))) {
            FerricStoreClient client =
                    FerricStoreClient.fromExecutor(new CapturingExecutor(response));
            assertThrows(
                    FerricStoreException.class,
                    () -> client.fetchOrCompute("cache", 1_000, null));
        }
    }

    @Test
    void carriesMaxActiveThroughEveryCreationSurfaceAndTypePolicy() {
        CapturingExecutor executor = new CapturingExecutor("OK", "OK", "OK", Map.of(), "OK");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.create(
                CreateOptions.builder("one", "order")
                        .nowMs(10)
                        .maxActiveMs(30_000)
                        .build());
        client.createMany(
                CreateManyOptions.builder(
                                "order",
                                List.of(
                                        new CreateItem(
                                                "two",
                                                "payload",
                                                "p",
                                                Map.of(),
                                                Map.of(),
                                                MaxActiveMs.infinity())))
                        .partitionKey("p")
                        .nowMs(10)
                        .maxActiveMs(60_000)
                        .build());
        client.spawnChildren(
                SpawnChildrenOptions.builder(
                                "one",
                                List.of(
                                        new ChildSpec(
                                                "child",
                                                "order",
                                                "payload",
                                                "p",
                                                Map.of(),
                                                Map.of(),
                                                MaxActiveMs.of(5_000))))
                        .partitionKey("p")
                        .nowMs(10)
                        .maxActiveMs(MaxActiveMs.infinity())
                        .build());
        client.startAndClaim(
                StartAndClaimOptions.builder("started", "order", "queued", "worker")
                        .nowMs(10)
                        .maxActiveMs(120_000)
                        .build());
        client.installPolicy(
                "order",
                FlowPolicyOptions.builder().maxActiveMs(MaxActiveMs.infinity()).build());

        assertOption(executor.calls.get(0), "MAX_ACTIVE_MS", 30_000L);
        assertOption(executor.calls.get(1), "MAX_ACTIVE_MS", 60_000L);
        assertEquals("ITEMS_MAPS", executor.calls.get(1).get(executor.calls.get(1).size() - 3));
        @SuppressWarnings("unchecked")
        Map<String, Object> createItem =
                (Map<String, Object>) executor.calls.get(1).getLast();
        assertEquals("infinity", createItem.get("max_active_ms"));
        assertOption(executor.calls.get(2), "MAX_ACTIVE_MS", "infinity");
        @SuppressWarnings("unchecked")
        Map<String, Object> child = (Map<String, Object>) executor.calls.get(2).getLast();
        assertEquals(5_000L, child.get("max_active_ms"));
        assertOption(executor.calls.get(3), "MAX_ACTIVE_MS", 120_000L);
        assertOption(executor.calls.get(4), "MAX_ACTIVE_MS", "infinity");

        assertThrows(IllegalArgumentException.class, () -> MaxActiveMs.of(0));
        assertThrows(IllegalArgumentException.class, () -> MaxActiveMs.of(31_536_000_001L));
    }

    @Test
    void decodesMaxActiveFailureAndRetainsUnknownFlowRecordExtensions() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", "flow");
        response.put("type", "order");
        response.put("state", "failed");
        response.put("fencing_token", 7L);
        response.put("version", 3L);
        response.put("max_active_ms", 500L);
        response.put("error", Map.of("reason", "max_active_ms", "max_active_ms", 500L));
        response.put("future_extension", Map.of("revision", 2L));

        FlowRecord record = Resp.record(response, new RawCodec());

        assertEquals("max_active_ms", record.failureReason());
        assertEquals(500L, record.maxActiveMs());
        assertEquals(Map.of("reason", "max_active_ms", "max_active_ms", 500L), record.error());
        assertEquals(Map.of("revision", 2L), record.raw().get("future_extension"));
    }

    @Test
    void mutationBuildersRequireThe08LeaseAndFencingInputs() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CompleteOptions.builder("flow", null, 1).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> RetryOptions.builder("flow", "", 1).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> FailOptions.builder("flow", null, 1).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> TransitionOptions.builder("flow", "a", "b", "", 1).build());

        CapturingExecutor executor = new CapturingExecutor("OK");
        FerricStoreClient.fromExecutor(executor)
                .cancel(CancelOptions.builder("flow", 8).nowMs(10).build());
        assertCommand(
                List.of("FLOW.CANCEL", "flow", "FENCING", 8L, "NOW", 10L),
                executor.calls.getFirst());
    }

    @Test
    void signalUsesCanonicalIdAndSignalFields() {
        CapturingExecutor executor = new CapturingExecutor("OK");
        FerricStoreClient.fromExecutor(executor)
                .signal("flow", "approved", null, "partition", Map.of());

        List<Object> command = executor.calls.getFirst();
        assertEquals("FLOW.SIGNAL", command.get(0));
        assertEquals("flow", command.get(1));
        assertOption(command, "SIGNAL", "approved");
        assertFalse(command.contains("FLOW_ID"));
    }

    @Test
    void setSupportsAbsoluteExpiryAndKeepTtlWithStrictExclusivity() {
        CapturingExecutor executor = new CapturingExecutor("OK", bytes("previous"));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        Object set =
                client.kv()
                        .set(
                                "key",
                                "value",
                                SetOptions.builder().pxatMillis(1_800_000_000_123L).nx(true).build());
        assertEquals(true, set);
        assertCommand(
                List.of(
                        "SET",
                        "key",
                        bytes("value"),
                        "PXAT",
                        1_800_000_000_123L,
                        "NX"),
                executor.calls.get(0));

        Object previous =
                client.kv()
                        .set(
                                "key",
                                "next",
                                SetOptions.builder().keepTtl(true).xx(true).get(true).build());
        assertArrayEquals(bytes("previous"), (byte[]) previous);
        assertCommand(
                List.of("SET", "key", bytes("next"), "XX", "GET", "KEEPTTL"),
                executor.calls.get(1));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SetOptions.builder()
                                .exSeconds(1)
                                .pxMilliseconds(2)
                                .build());
        assertThrows(
                IllegalArgumentException.class,
                () -> SetOptions.builder().exatSeconds(1).keepTtl(true).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> SetOptions.builder().nx(true).xx(true).build());
    }

    @Test
    void msetAndMsetnxRejectCrossSlotBeforeEncodingOrTransport() {
        CountingCodec codec = new CountingCodec();
        CapturingExecutor executor = new CapturingExecutor("OK", 1L);
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, codec);

        assertThrows(
                IllegalArgumentException.class,
                () -> client.kv().mset(Map.of("{one}:a", "a", "{two}:b", "b")));
        assertThrows(
                IllegalArgumentException.class,
                () -> client.kv().msetnx(Map.of("{one}:a", "a", "{two}:b", "b")));
        assertEquals(0, codec.encodes);
        assertTrue(executor.calls.isEmpty());

        assertTrue(client.kv().mset(Map.of("{same}:a", "a", "{same}:b", "b")));
        assertTrue(client.kv().msetnx(Map.of("{same}:c", "c", "{same}:d", "d")));
        assertEquals("MSET", executor.calls.get(0).getFirst());
        assertEquals("MSETNX", executor.calls.get(1).getFirst());
    }

    @Test
    void topKReserveHasOnlyTheCanonicalKWidthDepthShape() {
        CapturingExecutor executor = new CapturingExecutor("OK");
        TopKStore topk = FerricStoreClient.fromExecutor(executor).topk();

        assertTrue(
                topk.reserve(
                        "top",
                        10,
                        TopKReserveOptions.builder().width(8).depth(7).build()));
        assertCommand(
                List.of("TOPK.RESERVE", "top", 10L, 8L, 7L),
                executor.calls.getFirst());
        assertThrows(
                IllegalArgumentException.class,
                () -> TopKReserveOptions.builder().width(8).build());
        for (Method method : TopKReserveOptions.Builder.class.getMethods()) {
            assertFalse(method.getName().toLowerCase().contains("decay"));
        }
    }

    @Test
    void effectCommandsCarryAnExplicitPartitionOrFallBackToTheFlowId() {
        CapturingExecutor executor = new CapturingExecutor(Map.of(), Map.of());
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.effectReserve(
                "flow",
                "charge",
                "payment",
                EffectReserveOptions.builder("lease", 9)
                        .partitionKey("tenant")
                        .operationDigest("sha256:1")
                        .build());
        client.effectGet("flow", "charge", null);

        assertOption(executor.calls.get(0), "PARTITION", "tenant");
        assertEquals("flow", executor.calls.get(1).get(1));
        assertFalse(executor.calls.get(1).contains("PARTITION"));
        assertEquals("tenant", NativeRouting.routeKey(executor.calls.get(0)));
        assertEquals("flow", NativeRouting.routeKey(executor.calls.get(1)));
    }

    @Test
    void canonicalLineageApiDoesNotExposeParentOrRootAliases() {
        assertThrows(
                NoSuchMethodException.class,
                () -> CreateOptions.Builder.class.getMethod("parentId", String.class));
        assertThrows(
                NoSuchMethodException.class,
                () -> CreateOptions.Builder.class.getMethod("rootId", String.class));
        assertEquals(
                "parentFlowId",
                SpawnChildrenOptions.class.getRecordComponents()[0].getName());
    }

    private static void assertOption(List<Object> command, String name, Object value) {
        int index = command.indexOf(name);
        assertTrue(index >= 0, () -> "missing " + name + " in " + command);
        assertEquals(value, command.get(index + 1));
    }

    private static void assertCommand(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size(), "argument count");
        for (int index = 0; index < expected.size(); index++) {
            Object wanted = expected.get(index);
            Object got = actual.get(index);
            if (wanted instanceof byte[] wantedBytes && got instanceof byte[] gotBytes) {
                assertArrayEquals(wantedBytes, gotBytes, "argument " + index);
            } else {
                assertEquals(wanted, got, "argument " + index);
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class CapturingExecutor implements CommandExecutor {
        private final List<Object> responses = new ArrayList<>();
        private final List<List<Object>> calls = new ArrayList<>();

        private CapturingExecutor(Object response, Object... rest) {
            responses.add(response);
            responses.addAll(List.of(rest));
        }

        @Override
        public Object execute(List<Object> command) {
            calls.add(List.copyOf(command));
            return responses.get(Math.min(calls.size() - 1, responses.size() - 1));
        }
    }

    private static final class CountingCodec implements Codec {
        private int encodes;

        @Override
        public byte[] encode(Object value) {
            encodes++;
            return bytes(String.valueOf(value));
        }

        @Override
        public Object decode(byte[] value) {
            return value;
        }

        @Override
        public <T> T decode(byte[] value, Class<T> type) {
            return type.cast(value);
        }
    }
}
