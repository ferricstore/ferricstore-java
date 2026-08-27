package com.ferricstore;

import static com.ferricstore.CommandTestSupport.assertCommand;
import static com.ferricstore.CommandTestSupport.bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class AdvancedDataCommandContractTest {
    @Test
    void xaddSupportsTheCompleteTrimAndCreationOptionShape() {
        CapturingExecutor executor = new CapturingExecutor("1-0");
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new StringCodec());

        client.stream()
                .xadd(
                        "stream",
                        Map.of("field", "value"),
                        XAddOptions.builder()
                                .id("1-0")
                                .maxlen(100)
                                .approximate(true)
                                .noMkStream(true)
                                .build());

        assertCommand(
                List.of(
                        "XADD",
                        "stream",
                        "NOMKSTREAM",
                        "MAXLEN",
                        "~",
                        100L,
                        "1-0",
                        "field",
                        bytes("value")),
                executor.last());
        assertThrows(
                IllegalArgumentException.class,
                () -> XAddOptions.builder().maxlen(1).minid("1-0").build());
        assertThrows(
                IllegalArgumentException.class,
                () -> XAddOptions.builder().approximate(true).build());
    }

    @Test
    void sortedSetAndGeoConditionalWritesUseTypedOptions() {
        CapturingExecutor executor = new CapturingExecutor(1L, 1L);
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        client.zset()
                .zadd(
                        "zset",
                        List.of(new ZAddMember(1.5, "member")),
                        ZAddOptions.builder().nx(true).ch(true).build());
        client.geo()
                .geoadd(
                        "geo",
                        List.of(new GeoMember(1.0, 2.0, "member")),
                        GeoAddOptions.builder().xx(true).ch(true).build());

        assertCommand(
                List.of("ZADD", "zset", "NX", "CH", 1.5, bytes("member")), executor.calls.get(0));
        assertCommand(
                List.of("GEOADD", "geo", "XX", "CH", 1.0, 2.0, bytes("member")),
                executor.calls.get(1));
        assertThrows(
                IllegalArgumentException.class,
                () -> ZAddOptions.builder().nx(true).xx(true).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> GeoAddOptions.builder().nx(true).xx(true).build());
    }

    @Test
    void probabilisticFamiliesPreserveEveryServerOptionAndValidatePairs() {
        CapturingExecutor executor = new CapturingExecutor("OK", "OK", "OK", List.of(2L, 3L));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        assertTrue(client.bloom().reserve("bf", 0.01, 100, "EXPANSION", 2));
        assertTrue(client.cuckoo().reserve("cf", 100, "BUCKETSIZE", 4));
        assertTrue(client.tdigest().create("td", "COMPRESSION", 200));
        client.cms().incrBy("cms", "one", 2, "two", 3);

        assertCommand(
                List.of("BF.RESERVE", "bf", 0.01, 100L, "EXPANSION", 2), executor.calls.get(0));
        assertCommand(List.of("CF.RESERVE", "cf", 100L, "BUCKETSIZE", 4), executor.calls.get(1));
        assertCommand(List.of("TDIGEST.CREATE", "td", "COMPRESSION", 200), executor.calls.get(2));
        assertCommand(
                List.of("CMS.INCRBY", "cms", bytes("one"), 2, bytes("two"), 3),
                executor.calls.get(3));
        assertThrows(IllegalArgumentException.class, () -> client.cms().incrBy("cms", "orphan"));
    }

    @Test
    void hgetallNormalizesMapAndFlatResponsesAndDecodesOnlyValues() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(bytes("field"), bytes("value"));
        CapturingExecutor executor =
                new CapturingExecutor(map, List.of(bytes("other"), bytes("second")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new StringCodec());

        Map<Object, Object> mapped = client.hash().hgetall("hash");
        Map<Object, Object> flattened = client.hash().hgetall("hash");

        assertEquals("value", mapped.get(bytesKey(mapped, "field")));
        assertEquals("second", flattened.get(bytesKey(flattened, "other")));
    }

    @Test
    void jsonDocumentsUseSupportedCoreCommandsAndRejectUnsupportedPathMutation() {
        CapturingExecutor executor =
                new CapturingExecutor(
                        "OK",
                        bytes("{\"name\":\"Ada\"}"),
                        Arrays.asList(bytes("{\"name\":\"Ada\"}"), null),
                        1L);
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);

        assertTrue(client.json().set("doc", "$", Map.of("name", "Ada")));
        assertEquals(Map.of("name", "Ada"), client.json().get("doc", Map.class));
        assertEquals(2, client.json().mget(List.of("doc", "missing"), "$").size());
        assertEquals(1, client.json().del("doc", "$"));

        assertEquals("SET", executor.calls.get(0).get(0));
        assertEquals("GET", executor.calls.get(1).get(0));
        assertEquals("MGET", executor.calls.get(2).get(0));
        assertEquals("DEL", executor.calls.get(3).get(0));
        assertThrows(
                IllegalArgumentException.class, () -> client.json().set("doc", "$.name", "Grace"));
    }

    private static Object bytesKey(Map<Object, Object> map, String expected) {
        return map.keySet().stream()
                .filter(
                        key ->
                                key instanceof byte[] bytes
                                        && expected.equals(
                                                new String(bytes, StandardCharsets.UTF_8)))
                .findFirst()
                .orElseThrow();
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

        private List<Object> last() {
            return calls.get(calls.size() - 1);
        }
    }
}
