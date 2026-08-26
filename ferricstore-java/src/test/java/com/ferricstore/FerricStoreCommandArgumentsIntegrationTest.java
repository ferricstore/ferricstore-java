package com.ferricstore;

import static com.ferricstore.IntegrationTestEnvironment.assumeIntegration;
import static com.ferricstore.IntegrationTestEnvironment.connectRaw;
import static com.ferricstore.IntegrationTestEnvironment.suffix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Exercises the supported argument shapes, rather than merely proving that each command name is
 * accepted. The same class is run against native TCP and both HTTP wire formats.
 */
final class FerricStoreCommandArgumentsIntegrationTest {
    @Test
    void stringAndKeyCommandsSupportConditionalExpiryAndBulkArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            KeyValueStore kv = client.kv();
            String prefix = "{java-sdk:args:string:" + suffix() + "}:";
            long now = System.currentTimeMillis();

            try {
                String conditional = prefix + "conditional";
                assertEquals(
                        true,
                        kv.set(
                                conditional,
                                "first",
                                SetOptions.builder().nx(true).exSeconds(60).build()));
                assertNull(kv.set(conditional, "ignored", SetOptions.builder().nx(true).build()));
                assertNull(
                        kv.set(
                                prefix + "missing",
                                "ignored",
                                SetOptions.builder().xx(true).build()));
                assertEquals(
                        "first",
                        text(
                                kv.set(
                                        conditional,
                                        "second",
                                        SetOptions.builder().xx(true).get(true).build())));
                assertEquals("second", text(kv.get(conditional)));

                String keepTtl = prefix + "keep-ttl";
                assertEquals(
                        true,
                        kv.set(
                                keepTtl,
                                "before",
                                SetOptions.builder().pxMilliseconds(60_000).build()));
                assertEquals(
                        true, kv.set(keepTtl, "after", SetOptions.builder().keepTtl(true).build()));
                assertTrue(kv.pttl(keepTtl) > 0);

                assertExpiringSetVariant(
                        kv, prefix + "ex", SetOptions.builder().exSeconds(60).build());
                assertExpiringSetVariant(
                        kv, prefix + "px", SetOptions.builder().pxMilliseconds(60_000).build());
                assertExpiringSetVariant(
                        kv,
                        prefix + "exat",
                        SetOptions.builder().exatSeconds(now / 1_000 + 60).build());
                assertExpiringSetVariant(
                        kv, prefix + "pxat", SetOptions.builder().pxatMillis(now + 60_000).build());

                String expiry = prefix + "expiry";
                assertTrue(kv.set(expiry, "value"));
                assertTrue(kv.expire(expiry, 60, "NX"));
                assertFalse(kv.expire(expiry, 30, "NX"));
                assertTrue(kv.expire(expiry, 120, "GT"));
                assertTrue(kv.expire(expiry, 60, "LT"));
                assertTrue(kv.pexpire(expiry, 90_000, "XX"));
                assertTrue(kv.expireAt(expiry, now / 1_000 + 120, "GT"));
                assertTrue(kv.pexpireAt(expiry, now + 60_000, "LT"));

                String getex = prefix + "getex";
                assertTrue(kv.set(getex, "value"));
                assertEquals("value", text(kv.getex(getex)));
                assertEquals("value", text(kv.getex(getex, "EX", 60)));
                assertEquals("value", text(kv.getex(getex, "PX", 60_000)));
                assertEquals("value", text(kv.getex(getex, "EXAT", now / 1_000 + 60)));
                assertEquals("value", text(kv.getex(getex, "PXAT", now + 60_000)));
                assertEquals("value", text(kv.getex(getex, "PERSIST")));
                assertEquals(-1, kv.ttl(getex));

                Map<String, Object> bulk = Map.of(prefix + "bulk-a", "a", prefix + "bulk-b", "b");
                assertTrue(kv.mset(bulk));
                assertEquals(
                        List.of("a", "b"),
                        texts(kv.mget(List.of(prefix + "bulk-a", prefix + "bulk-b"))));
                assertFalse(kv.msetnx(Map.of(prefix + "bulk-a", "x", prefix + "bulk-c", "c")));
                assertEquals(2, kv.exists(prefix + "bulk-a", prefix + "bulk-b", prefix + "absent"));
                assertNotNull(kv.scan("0", prefix + "bulk-*", 1L));
                assertEquals(2, kv.unlink(prefix + "bulk-a", prefix + "bulk-b"));
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void hashAndListCommandsSupportMultiFieldCountSearchAndBlockingArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:collections:" + suffix() + "}:";

            try {
                HashStore hash = client.hash();
                String hashKey = prefix + "hash";
                assertEquals(3, hash.hset(hashKey, Map.of("a", "1", "b", "2", "c", "3")));
                assertEquals(
                        Arrays.asList("1", "2", null), texts(hash.hmget(hashKey, "a", "b", "z")));
                assertNotNull(hash.hrandfield(hashKey));
                assertEquals(2, list(hash.hrandfield(hashKey, 2)).size());
                assertEquals(4, list(hash.hrandfield(hashKey, 2, "WITHVALUES")).size());
                assertNotNull(hash.hscan(hashKey, 0, "MATCH", "[ab]", "COUNT", 1));
                assertNotNull(hash.hexpire(hashKey, 60, "a", "b"));
                assertNotNull(hash.httl(hashKey, "a", "b"));
                assertEquals(
                        List.of("1", "2"),
                        texts(list(hash.hgetex(hashKey, "PX", 60_000, "FIELDS", 2, "a", "b"))));
                assertNotNull(hash.hpersist(hashKey, "a", "b"));
                assertEquals(List.of("2", "3"), texts(hash.hgetdel(hashKey, "b", "c")));

                ListStore lists = client.lists();
                String popLeft = prefix + "pop-left";
                assertEquals(4, lists.rpush(popLeft, "a", "b", "c", "d"));
                assertEquals("a", text(lists.lpop(popLeft)));
                assertEquals(List.of("b", "c"), texts(lists.lpop(popLeft, 2)));

                String popRight = prefix + "pop-right";
                assertEquals(4, lists.rpush(popRight, "a", "b", "c", "d"));
                assertEquals("d", text(lists.rpop(popRight)));
                assertEquals(List.of("c", "b"), texts(lists.rpop(popRight, 2)));

                String positions = prefix + "positions";
                assertEquals(5, lists.rpush(positions, "a", "b", "a", "c", "a"));
                assertEquals(2, number(lists.lpos(positions, "a", "RANK", 2)));
                assertEquals(
                        List.of(0L, 2L, 4L), numbers(list(lists.lpos(positions, "a", "COUNT", 0))));
                assertEquals(
                        List.of(2L, 4L),
                        numbers(
                                list(
                                        lists.lpos(
                                                positions, "a", "RANK", 2, "COUNT", 2, "MAXLEN",
                                                5))));
                assertEquals(6, lists.linsert(positions, "BEFORE", "b", "before-b"));
                assertEquals(7, lists.linsert(positions, "AFTER", "b", "after-b"));

                String blocking = prefix + "blocking";
                assertEquals(1, lists.rpush(blocking, "ready"));
                List<Object> popped = list(lists.blpop(List.of(prefix + "absent", blocking), 1));
                assertEquals(blocking, text(popped.get(0)));
                assertEquals("ready", text(popped.get(1)));

                String blockingMove = prefix + "blocking-move";
                String blockingDestination = prefix + "blocking-destination";
                assertEquals(2, lists.rpush(blockingMove, "one", "two"));
                assertEquals(
                        "one",
                        text(lists.blmove(blockingMove, blockingDestination, "LEFT", "RIGHT", 1)));
                assertNotNull(lists.blmpop(1, List.of(blockingMove), "RIGHT", 1));
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void setAndSortedSetCommandsSupportOptionalCountRangeAndConditionalArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:set:" + suffix() + "}:";

            try {
                SetStore sets = client.sets();
                String setA = prefix + "set-a";
                String setB = prefix + "set-b";
                assertEquals(4, sets.sadd(setA, "a", "b", "c", "d"));
                assertEquals(3, sets.sadd(setB, "b", "c", "e"));
                assertNotNull(sets.srandmember(setA, null));
                assertEquals(2, list(sets.srandmember(setA, 2)).size());
                assertEquals(6, list(sets.srandmember(setA, -6)).size());
                assertEquals(2, sets.sintercard(List.of(setA, setB), null));
                assertEquals(1, sets.sintercard(List.of(setA, setB), 1));
                assertNotNull(sets.sscan(setA, 0, "MATCH", "[ab]", "COUNT", 1));

                String popOne = prefix + "set-pop-one";
                assertEquals(2, sets.sadd(popOne, "a", "b"));
                assertNotNull(sets.spop(popOne, null));
                String popMany = prefix + "set-pop-many";
                assertEquals(3, sets.sadd(popMany, "a", "b", "c"));
                assertEquals(2, list(sets.spop(popMany, 2)).size());

                SortedSetStore zset = client.zset();
                String zsetKey = prefix + "zset";
                assertEquals(
                        3,
                        zset.zadd(
                                zsetKey,
                                List.of(
                                        new ZAddMember(1, "a"),
                                        new ZAddMember(2, "b"),
                                        new ZAddMember(3, "c"))));
                assertEquals(
                        0,
                        number(
                                zset.zadd(
                                        zsetKey,
                                        List.of(new ZAddMember(9, "a")),
                                        ZAddOptions.builder().nx(true).build())));
                assertEquals("1.0", zset.zscore(zsetKey, "a"));
                assertEquals(
                        1,
                        number(
                                zset.zadd(
                                        zsetKey,
                                        List.of(new ZAddMember(4, "a")),
                                        ZAddOptions.builder().xx(true).ch(true).gt(true).build())));
                assertEquals(
                        1,
                        number(
                                zset.zadd(
                                        zsetKey,
                                        List.of(new ZAddMember(0.5, "a")),
                                        ZAddOptions.builder().xx(true).ch(true).lt(true).build())));
                assertEquals(2.5, zset.zincrBy(zsetKey, 0.5, "b"));

                assertEquals(6, zset.zrange(zsetKey, 0, -1, "WITHSCORES").size());
                assertEquals(6, zset.zrevrange(zsetKey, 0, -1, "WITHSCORES").size());
                assertEquals(
                        4,
                        zset.zrangeByScore(zsetKey, "(0.5", "+inf", "WITHSCORES", "LIMIT", 0, 2)
                                .size());
                assertEquals(
                        2, zset.zrevrangeByScore(zsetKey, "+inf", "-inf", "LIMIT", 1, 2).size());
                assertNotNull(zset.zrandmember(zsetKey));
                assertEquals(2, list(zset.zrandmember(zsetKey, 2)).size());
                assertEquals(4, list(zset.zrandmember(zsetKey, 2, "WITHSCORES")).size());
                assertNotNull(zset.zscan(zsetKey, 0, "MATCH", "[ab]", "COUNT", 1));

                String popMin = prefix + "zpop-min";
                zset.zadd(popMin, List.of(new ZAddMember(1, "a"), new ZAddMember(2, "b")));
                assertEquals(2, zset.zpopmin(popMin, null).size());
                String popMax = prefix + "zpop-max";
                zset.zadd(
                        popMax,
                        List.of(
                                new ZAddMember(1, "a"),
                                new ZAddMember(2, "b"),
                                new ZAddMember(3, "c")));
                assertEquals(4, zset.zpopmax(popMax, 2).size());
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void streamCommandsSupportTrimReadGroupAndBlockingArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:stream:" + suffix() + "}:";

            try {
                StreamStore streams = client.stream();
                String noMkStream = prefix + "stream-no-mk";
                assertNull(
                        streams.xadd(
                                noMkStream,
                                Map.of("field", "value"),
                                XAddOptions.builder().noMkStream(true).build()));

                String stream = prefix + "stream";
                assertNotNull(streams.xadd(stream, "1-0", Map.of("field", "one")));
                assertNotNull(
                        streams.xadd(
                                stream,
                                Map.of("field", "two"),
                                XAddOptions.builder()
                                        .id("2-0")
                                        .maxlen(10)
                                        .approximate(true)
                                        .build()));
                assertNotNull(
                        streams.xadd(
                                stream,
                                Map.of("field", "three"),
                                XAddOptions.builder().id("3-0").noMkStream(true).build()));
                assertEquals(1, streams.xrange(stream, "-", "+", "COUNT", 1).size());
                assertEquals(2, streams.xrevrange(stream, "+", "-", "COUNT", 2).size());
                assertNotNull(streams.xread(Map.of(stream, "0-0"), null, null));
                assertNotNull(streams.xread(Map.of(stream, "0-0"), 1, 1L));
                assertTrue(number(streams.xtrim(stream, "MAXLEN", "=", 2)) >= 0);

                String grouped = prefix + "stream-group";
                String group = "group";
                assertTrue(ok(streams.xgroup("CREATE", grouped, group, "0", "MKSTREAM")));
                String groupedId = text(streams.xadd(grouped, "*", Map.of("field", "value")));
                assertNotNull(
                        streams.xreadgroup(group, "consumer", Map.of(grouped, ">"), 1, 1L, false));
                assertEquals(1, streams.xack(grouped, group, groupedId));
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void bitmapAndHyperLogLogCommandsSupportOperationAndMultiKeyArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:binary:" + suffix() + "}:";

            try {
                BitmapStore bitmap = client.bitmap();
                String bitmapA = prefix + "bitmap-a";
                String bitmapB = prefix + "bitmap-b";
                bitmap.setbit(bitmapA, 0, 1);
                bitmap.setbit(bitmapA, 7, 1);
                bitmap.setbit(bitmapB, 1, 1);
                assertEquals(2, bitmap.bitcount(bitmapA));
                assertEquals(0, bitmap.bitpos(bitmapA, 1, 0, 0));
                assertEquals(1, bitmap.bitop("OR", prefix + "bitmap-or", bitmapA, bitmapB));
                assertEquals(1, bitmap.bitop("AND", prefix + "bitmap-and", bitmapA, bitmapB));
                assertEquals(1, bitmap.bitop("XOR", prefix + "bitmap-xor", bitmapA, bitmapB));
                assertEquals(1, bitmap.bitop("NOT", prefix + "bitmap-not", bitmapA));

                HyperLogLogStore hll = client.hyperloglog();
                String hllA = prefix + "hll-a";
                String hllB = prefix + "hll-b";
                assertEquals(1, hll.pfadd(hllA, "a", "b"));
                assertEquals(1, hll.pfadd(hllB, "b", "c"));
                assertTrue(hll.pfcount(hllA, hllB) >= 3);
                assertTrue(hll.pfmerge(prefix + "hll-merged", hllA, hllB));
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void geoCommandsSupportConditionalSearchShapeResultAndStoreArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:geo:" + suffix() + "}:";

            try {
                GeoStore geo = client.geo();
                String geoKey = prefix + "geo";
                GeoMember palermo = new GeoMember(13.361389, 38.115556, "palermo");
                GeoMember catania = new GeoMember(15.087269, 37.502669, "catania");
                assertEquals(2, geo.geoadd(geoKey, List.of(palermo, catania)));
                assertEquals(
                        0,
                        geo.geoadd(
                                geoKey,
                                List.of(new GeoMember(14, 38, "palermo")),
                                GeoAddOptions.builder().nx(true).build()));
                assertEquals(
                        1,
                        geo.geoadd(
                                geoKey,
                                List.of(new GeoMember(13.4, 38.1, "palermo")),
                                GeoAddOptions.builder().xx(true).ch(true).build()));
                assertNotNull(geo.geodist(geoKey, "palermo", "catania", null));
                assertNotNull(geo.geodist(geoKey, "palermo", "catania", "km"));
                assertEquals(
                        2,
                        geo.geopos(geoKey, "palermo", "missing") instanceof List<?> values
                                ? values.size()
                                : 0);
                assertNotNull(
                        geo.geosearch(
                                geoKey,
                                "FROMMEMBER",
                                "palermo",
                                "BYRADIUS",
                                300,
                                "km",
                                "WITHDIST",
                                "ASC",
                                "COUNT",
                                1));
                assertNotNull(
                        geo.geosearch(
                                geoKey,
                                "FROMLONLAT",
                                13.5,
                                38.0,
                                "BYBOX",
                                400,
                                400,
                                "km",
                                "WITHCOORD",
                                "DESC"));
                assertTrue(
                        geo.geosearchstore(
                                        prefix + "geo-store",
                                        geoKey,
                                        "FROMMEMBER",
                                        "palermo",
                                        "BYRADIUS",
                                        300,
                                        "km")
                                >= 1);
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void probabilisticCommandsSupportSingleMultiAndOptionArgumentVariants() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String prefix = "{java-sdk:args:prob:" + suffix() + "}:";

            try {
                String bloom = prefix + "bf";
                assertTrue(client.bloom().reserve(bloom, 0.01, 100));
                assertEquals(3, client.bloom().madd(bloom, "a", "b", "c").size());
                assertEquals(3, client.bloom().mexists(bloom, "a", "z", "c").size());

                String cuckoo = prefix + "cf";
                assertTrue(client.cuckoo().reserve(cuckoo, 100));
                assertTrue(client.cuckoo().add(cuckoo, "a"));
                assertFalse(client.cuckoo().addnx(cuckoo, "a"));
                assertEquals(3, client.cuckoo().mexists(cuckoo, "a", "z", "b").size());

                String cmsA = prefix + "cms-a";
                String cmsB = prefix + "cms-b";
                assertTrue(client.cms().initByDim(cmsA, 20, 4));
                assertTrue(client.cms().initByDim(cmsB, 20, 4));
                assertEquals(2, client.cms().incrBy(cmsA, "a", 2, "b", 3).size());
                assertEquals(2, client.cms().incrBy(cmsB, "a", 1, "b", 1).size());
                assertTrue(
                        client.cms()
                                .merge(prefix + "cms-weighted", 2, cmsA, cmsB, "WEIGHTS", 2, 1));
                assertEquals(
                        List.of(5L, 7L),
                        client.cms().query(prefix + "cms-weighted", "a", "b").stream()
                                .map(FerricStoreCommandArgumentsIntegrationTest::number)
                                .toList());

                String topk = prefix + "topk";
                assertTrue(
                        client.topk()
                                .reserve(
                                        topk,
                                        3,
                                        TopKReserveOptions.builder().width(20).depth(5).build()));
                assertEquals(4, client.topk().add(topk, "a", "b", "a", "c").size());
                assertEquals(2, client.topk().incrBy(topk, "d", 2, "e", 1).size());
                assertFalse(client.topk().list(topk, false).isEmpty());
                assertFalse(client.topk().list(topk, true).isEmpty());

                String tdigest = prefix + "tdigest";
                assertTrue(client.tdigest().create(tdigest, "COMPRESSION", 200));
                assertTrue(client.tdigest().add(tdigest, 1, 2, 3, 4, 5));
                assertEquals(3, client.tdigest().quantile(tdigest, 0.0, 0.5, 1.0).size());
                assertEquals(3, client.tdigest().cdf(tdigest, 1, 3, 5).size());
                assertEquals(2, client.tdigest().rank(tdigest, 2, 4).size());
                assertEquals(2, client.tdigest().reverseRank(tdigest, 2, 4).size());
                assertEquals(2, client.tdigest().byRank(tdigest, 0, 2).size());
                assertEquals(2, client.tdigest().byReverseRank(tdigest, 0, 2).size());
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    private static void assertExpiringSetVariant(KeyValueStore kv, String key, SetOptions options) {
        assertEquals(true, kv.set(key, "value", options));
        assertTrue(kv.pttl(key) > 0);
    }

    private static List<Object> list(Object value) {
        return Resp.list(value);
    }

    private static long number(Object value) {
        return Resp.number(value);
    }

    private static List<Long> numbers(List<?> values) {
        return values.stream().map(FerricStoreCommandArgumentsIntegrationTest::number).toList();
    }

    private static boolean ok(Object value) {
        return CommandArgs.ok(value);
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Resp.string(value);
    }

    private static List<String> texts(List<?> values) {
        return values.stream().map(value -> value == null ? null : text(value)).toList();
    }

    private static void deletePrefixedKeys(FerricStoreClient client, String prefix) {
        List<Object> keys = client.kv().keys(prefix + "*");
        for (int start = 0; start < keys.size(); start += 64) {
            deleteKeyRange(client, keys, start, Math.min(start + 64, keys.size()));
        }
    }

    private static void deleteKeyRange(
            FerricStoreClient client, List<Object> keys, int start, int end) {
        List<Object> command = new java.util.ArrayList<>();
        command.add("DEL");
        command.addAll(keys.subList(start, end));
        client.command(command);
    }
}
