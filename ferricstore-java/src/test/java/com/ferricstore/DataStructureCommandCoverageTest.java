package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class DataStructureCommandCoverageTest {
    private static final String KEY = "{coverage}a";
    private static final String OTHER_KEY = "{coverage}b";

    @Test
    void keyValueSurfaceBuildsEverySupportedCommandShape() {
        RespondingExecutor executor = new RespondingExecutor();
        KeyValueStore store = client(executor).kv();

        assertEquals("value", store.get(KEY));
        assertEquals("value", store.get(KEY, String.class));
        assertTrue(store.set(KEY, "value"));
        assertEquals(
                "value",
                store.set(
                        KEY,
                        "next",
                        SetOptions.builder().exSeconds(10).nx(true).get(true).build()));
        store.set(KEY, "value", SetOptions.builder().pxMilliseconds(10).xx(true).build());
        store.set(KEY, "value", SetOptions.builder().exatSeconds(10).build());
        store.set(KEY, "value", SetOptions.builder().pxatMillis(10).build());
        store.set(KEY, "value", SetOptions.builder().keepTtl(true).build());
        store.del(KEY, OTHER_KEY);
        store.exists(KEY, OTHER_KEY);
        store.mget(List.of(KEY, OTHER_KEY));
        store.mset(Map.of(KEY, "one", OTHER_KEY, "two"));
        store.msetnx(Map.of(KEY, "one", OTHER_KEY, "two"));
        store.incr(KEY);
        store.decr(KEY);
        store.incrBy(KEY, 2);
        store.decrBy(KEY, 2);
        store.incrByFloat(KEY, 1.5);
        store.append(KEY, "tail");
        store.strlen(KEY);
        store.getdel(KEY);
        store.getex(KEY, "PX", 10);
        store.setnx(KEY, "value");
        store.setex(KEY, 10, "value");
        store.psetex(KEY, 10, "value");
        store.getrange(KEY, 0, 1);
        store.setrange(KEY, 1, "value");
        store.expire(KEY, 10);
        store.expire(KEY, 10, "nx");
        store.pexpire(KEY, 10);
        store.pexpire(KEY, 10, "xx");
        store.expireAt(KEY, 10);
        store.expireAt(KEY, 10, "gt");
        store.pexpireAt(KEY, 10);
        store.pexpireAt(KEY, 10, "lt");
        store.ttl(KEY);
        store.pttl(KEY);
        store.persist(KEY);
        store.expireTime(KEY);
        store.pexpireTime(KEY);
        store.type(KEY);
        store.keys("*");
        store.scan("0", "prefix:*", 10L);
        store.scan("0", null, null);
        store.unlink(KEY, OTHER_KEY);
        store.rename(KEY, OTHER_KEY);
        store.renamenx(KEY, OTHER_KEY);
        store.copy(KEY, OTHER_KEY, "REPLACE");
        store.randomKey();
        store.object("ENCODING", KEY);

        assertTrue(executor.commandNames().containsAll(Set.of("GET", "SET", "MGET", "SCAN")));
        assertThrows(IllegalArgumentException.class, () -> store.expire(KEY, 1, "invalid"));
        assertThrows(IllegalArgumentException.class, () -> store.mset(Map.of()));
    }

    @Test
    @SuppressWarnings("PMD.NcssCount") // One exhaustive typed-command audit per related family.
    void listSetAndHashSurfacesBuildEverySupportedCommandShape() {
        RespondingExecutor executor = new RespondingExecutor();
        FerricStoreClient client = client(executor);
        ListStore list = client.lists();
        SetStore set = client.sets();
        HashStore hash = client.hash();

        list.lpush(KEY, "one", "two");
        list.rpush(KEY, "one", "two");
        list.lpop(KEY);
        list.lpop(KEY, 2);
        list.rpop(KEY);
        list.rpop(KEY, 2);
        list.lrange(KEY, 0, -1);
        list.llen(KEY);
        list.lindex(KEY, 0);
        list.lset(KEY, 0, "value");
        list.lrem(KEY, 1, "value");
        list.ltrim(KEY, 0, 1);
        list.lpos(KEY, "value", "RANK", 1);
        list.linsert(KEY, "before", "pivot", "value");
        list.lmove(KEY, OTHER_KEY, "LEFT", "RIGHT");
        list.blpop(List.of(KEY, OTHER_KEY), 1);
        list.brpop(List.of(KEY, OTHER_KEY), 1);
        list.blmove(KEY, OTHER_KEY, "LEFT", "RIGHT", 1.5);
        list.blmpop(1.5, List.of(KEY, OTHER_KEY), "LEFT", 2);
        list.lpushx(KEY, "value");
        list.rpushx(KEY, "value");
        list.rpoplpush(KEY, OTHER_KEY);

        set.sadd(KEY, "one", "two");
        set.srem(KEY, "one", "two");
        set.smembers(KEY);
        set.sismember(KEY, "one");
        set.smismember(KEY, "one", "two");
        set.scard(KEY);
        set.sinter(KEY, OTHER_KEY);
        set.sunion(KEY, OTHER_KEY);
        set.sdiff(KEY, OTHER_KEY);
        set.sdiffstore(KEY, OTHER_KEY);
        set.sinterstore(KEY, OTHER_KEY);
        set.sunionstore(KEY, OTHER_KEY);
        set.sintercard(List.of(KEY, OTHER_KEY), 1);
        set.srandmember(KEY, null);
        set.srandmember(KEY, 2);
        set.spop(KEY, null);
        set.spop(KEY, 2);
        set.smove(KEY, OTHER_KEY, "one");
        set.sscan(KEY, 0, "COUNT", 10);

        hash.hset(KEY, Map.of("one", "first", "two", "second"));
        hash.hget(KEY, "one");
        hash.hmget(KEY, "one", "two");
        hash.hdel(KEY, "one", "two");
        hash.hgetall(KEY);
        hash.hexists(KEY, "one");
        hash.hkeys(KEY);
        hash.hvals(KEY);
        hash.hlen(KEY);
        hash.hincrBy(KEY, "one", 2);
        hash.hincrByFloat(KEY, "one", 1.5);
        hash.hsetnx(KEY, "one", "value");
        hash.hstrlen(KEY, "one");
        hash.hrandfield(KEY, 2, "WITHVALUES");
        hash.hscan(KEY, 0, "COUNT", 10);
        hash.httl(KEY, "one");
        hash.hpttl(KEY, "one");
        hash.hpersist(KEY, "one");
        hash.hexpire(KEY, 10, "one");
        hash.hpexpire(KEY, 10, "one");
        hash.hexpireTime(KEY, "one");
        hash.hgetdel(KEY, "one");
        hash.hgetex(KEY, "EX", 10, "FIELDS", 1, "one");
        hash.hsetex(KEY, "EX", 10, "FIELDS", 1, "one", "value");

        assertTrue(executor.commandNames().containsAll(Set.of("BLMPOP", "SINTERCARD", "HGETEX")));
        assertThrows(IllegalArgumentException.class, () -> list.linsert(KEY, "middle", "a", "b"));
        assertThrows(IllegalArgumentException.class, () -> set.sintercard(List.of(), null));
        assertThrows(IllegalArgumentException.class, () -> hash.httl(KEY));
    }

    @Test
    @SuppressWarnings("PMD.NcssCount") // One exhaustive typed-command audit per related family.
    void sortedSetStreamGeoBitmapAndHyperLogLogSurfacesAreComplete() {
        RespondingExecutor executor = new RespondingExecutor();
        FerricStoreClient client = client(executor);
        SortedSetStore sorted = client.zset();
        StreamStore stream = client.stream();
        GeoStore geo = client.geo();
        BitmapStore bitmap = client.bitmap();
        HyperLogLogStore hyperLogLog = client.hyperloglog();

        List<ZAddMember> members = List.of(new ZAddMember(1.5, "one"));
        sorted.zadd(KEY, members);
        sorted.zadd(
                KEY, members, ZAddOptions.builder().xx(true).gt(true).ch(true).incr(true).build());
        sorted.zscore(KEY, "one");
        sorted.zrange(KEY, 0, -1);
        sorted.zrange(KEY, 0, -1, "WITHSCORES");
        sorted.zrevrange(KEY, 0, -1, "WITHSCORES");
        sorted.zrem(KEY, "one", "two");
        sorted.zcard(KEY);
        sorted.zincrBy(KEY, 1.5, "one");
        sorted.zcount(KEY, "-inf", "+inf");
        sorted.zrank(KEY, "one");
        sorted.zrevrank(KEY, "one");
        sorted.zmscore(KEY, "one", "two");
        sorted.zpopmin(KEY, null);
        sorted.zpopmax(KEY, 2);
        sorted.zrandmember(KEY);
        sorted.zrandmember(KEY, 2, "WITHSCORES");
        sorted.zscan(KEY, 0, "COUNT", 10);
        sorted.zrangeByScore(KEY, "-inf", "+inf", "LIMIT", 0, 10);
        sorted.zrevrangeByScore(KEY, "+inf", "-inf", "LIMIT", 0, 10);

        stream.xadd(KEY, "*", Map.of("field", "value"));
        stream.xadd(
                KEY,
                Map.of("field", "value"),
                XAddOptions.builder()
                        .id("1-0")
                        .minid("0-0")
                        .approximate(true)
                        .noMkStream(true)
                        .build());
        stream.xlen(KEY);
        stream.xrange(KEY, "-", "+");
        stream.xrange(KEY, "-", "+", "COUNT", 10);
        stream.xrevrange(KEY, "+", "-", "COUNT", 10);
        stream.xread(Map.of(KEY, "0-0", OTHER_KEY, "$"), 10, 100L);
        stream.xtrim(KEY, "MAXLEN", 10);
        stream.xdel(KEY, "1-0", "2-0");
        stream.xinfo("STREAM", KEY, "FULL");
        stream.xgroup("CREATE", KEY, "group", "$", "MKSTREAM");
        stream.xreadgroup("group", "consumer", Map.of(KEY, ">", OTHER_KEY, ">"), 10, 100L, true);
        stream.xack(KEY, "group", "1-0");

        List<GeoMember> geoMembers = List.of(new GeoMember(1.0, 2.0, "one"));
        geo.geoadd(KEY, geoMembers);
        geo.geoadd(KEY, geoMembers, GeoAddOptions.builder().nx(true).ch(true).build());
        geo.geopos(KEY, "one", "two");
        geo.geodist(KEY, "one", "two", "km");
        geo.geohash(KEY, "one", "two");
        geo.geosearch(KEY, "FROMMEMBER", "one", "BYRADIUS", 10, "km");
        geo.geosearchstore(OTHER_KEY, KEY, "FROMMEMBER", "one", "BYRADIUS", 10, "km");

        bitmap.setbit(KEY, 1, 1);
        bitmap.getbit(KEY, 1);
        bitmap.bitcount(KEY);
        bitmap.bitcount(KEY, 0, 1, "BYTE");
        bitmap.bitpos(KEY, 1, 0, 1, "BYTE");
        bitmap.bitop("AND", KEY, OTHER_KEY);

        hyperLogLog.pfadd(KEY, "one", "two");
        hyperLogLog.pfcount(KEY, OTHER_KEY);
        hyperLogLog.pfmerge(KEY, OTHER_KEY);

        assertTrue(
                executor.commandNames()
                        .containsAll(Set.of("ZREVRANGEBYSCORE", "XREADGROUP", "GEOSEARCHSTORE")));
        assertThrows(IllegalArgumentException.class, () -> stream.xread(Map.of(), null, null));
    }

    @Test
    void probabilisticDataStructureSurfacesBuildEverySupportedCommandShape() {
        RespondingExecutor executor = new RespondingExecutor();
        FerricStoreClient client = client(executor);

        BloomFilterStore bloom = client.bloom();
        bloom.reserve(KEY, 0.01, 100, "EXPANSION", 2);
        bloom.add(KEY, "one");
        bloom.madd(KEY, "one", "two");
        bloom.exists(KEY, "one");
        bloom.mexists(KEY, "one", "two");
        bloom.card(KEY);
        bloom.info(KEY);

        CuckooFilterStore cuckoo = client.cuckoo();
        cuckoo.reserve(KEY, 100, "BUCKETSIZE", 4);
        cuckoo.add(KEY, "one");
        cuckoo.addnx(KEY, "one");
        cuckoo.del(KEY, "one");
        cuckoo.exists(KEY, "one");
        cuckoo.mexists(KEY, "one", "two");
        cuckoo.count(KEY, "one");
        cuckoo.info(KEY);

        CountMinSketchStore cms = client.cms();
        cms.initByDim(KEY, 100, 5);
        cms.initByProb(KEY, 0.01, 0.01);
        cms.incrBy(KEY, "one", 2);
        cms.incrBy(KEY, "one", 2, "two", 3);
        cms.query(KEY, "one", "two");
        cms.merge(KEY, 1, OTHER_KEY);
        cms.info(KEY);

        TopKStore topK = client.topk();
        topK.reserve(KEY, 10);
        topK.reserve(KEY, 10, TopKReserveOptions.builder().width(100).depth(5).build());
        topK.add(KEY, "one", "two");
        topK.query(KEY, "one", "two");
        topK.incrBy(KEY, "one", 2, "two", 3);
        topK.list(KEY, false);
        topK.list(KEY, true);
        topK.count(KEY, "one", "two");
        topK.info(KEY);

        TDigestStore digest = client.tdigest();
        digest.create(KEY, "COMPRESSION", 100);
        digest.add(KEY, 1.0, 2.0);
        digest.quantile(KEY, 0.5, 0.9);
        digest.reset(KEY);
        digest.cdf(KEY, 1.0, 2.0);
        digest.rank(KEY, 1.0, 2.0);
        digest.reverseRank(KEY, 1.0, 2.0);
        digest.byRank(KEY, 1, 2);
        digest.byReverseRank(KEY, 1, 2);
        digest.trimmedMean(KEY, 0.1, 0.9);
        digest.min(KEY);
        digest.max(KEY);
        digest.merge(KEY, 1, OTHER_KEY);
        digest.info(KEY);

        assertTrue(
                executor.commandNames()
                        .containsAll(
                                Set.of(
                                        "BF.MEXISTS",
                                        "CF.ADDNX",
                                        "CMS.INITBYPROB",
                                        "TOPK.INCRBY",
                                        "TDIGEST.BYREVRANK")));
    }

    private static FerricStoreClient client(RespondingExecutor executor) {
        return FerricStoreClient.fromExecutor(executor, new StringCodec());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RespondingExecutor implements CommandExecutor {
        private static final Set<String> OK_COMMANDS =
                Set.of(
                        "SET",
                        "MSET",
                        "SETEX",
                        "PSETEX",
                        "RENAME",
                        "LSET",
                        "LTRIM",
                        "PFMERGE",
                        "BF.RESERVE",
                        "CF.RESERVE",
                        "CMS.INITBYDIM",
                        "CMS.INITBYPROB",
                        "CMS.MERGE",
                        "TOPK.RESERVE",
                        "TDIGEST.CREATE",
                        "TDIGEST.ADD",
                        "TDIGEST.RESET",
                        "TDIGEST.MERGE");
        private static final Set<String> LIST_COMMANDS =
                Set.of(
                        "MGET",
                        "KEYS",
                        "LRANGE",
                        "SMEMBERS",
                        "SMISMEMBER",
                        "SINTER",
                        "SUNION",
                        "SDIFF",
                        "HMGET",
                        "HKEYS",
                        "HVALS",
                        "HGETDEL",
                        "ZRANGE",
                        "ZREVRANGE",
                        "ZRANGEBYSCORE",
                        "ZREVRANGEBYSCORE",
                        "ZMSCORE",
                        "ZPOPMIN",
                        "ZPOPMAX",
                        "XRANGE",
                        "XREVRANGE",
                        "GEOHASH",
                        "BF.MADD",
                        "BF.MEXISTS",
                        "CF.MEXISTS",
                        "CMS.INCRBY",
                        "CMS.QUERY",
                        "TOPK.ADD",
                        "TOPK.QUERY",
                        "TOPK.INCRBY",
                        "TOPK.LIST",
                        "TOPK.COUNT",
                        "TDIGEST.QUANTILE",
                        "TDIGEST.CDF",
                        "TDIGEST.RANK",
                        "TDIGEST.REVRANK",
                        "TDIGEST.BYRANK",
                        "TDIGEST.BYREVRANK");

        private final List<List<Object>> calls = new ArrayList<>();

        @Override
        public Object execute(List<Object> command) {
            List<Object> copy = List.copyOf(command);
            calls.add(copy);
            String name = String.valueOf(copy.get(0));
            if ("SET".equals(name) && copy.contains("GET")) {
                return bytes("value");
            }
            if (OK_COMMANDS.contains(name)) {
                return "OK";
            }
            if ("GET".equals(name)
                    || "GETDEL".equals(name)
                    || "GETEX".equals(name)
                    || "HGET".equals(name)
                    || "LINDEX".equals(name)
                    || "RPOPLPUSH".equals(name)
                    || "LMOVE".equals(name)
                    || "RANDOMKEY".equals(name)
                    || "TYPE".equals(name)) {
                return bytes("value");
            }
            if ("HGETALL".equals(name)) {
                return List.of("field", bytes("value"));
            }
            if (("LPOP".equals(name) || "RPOP".equals(name)) && copy.size() == 2) {
                return bytes("value");
            }
            if (("SRANDMEMBER".equals(name) || "SPOP".equals(name) || "ZRANDMEMBER".equals(name))
                    && copy.size() == 2) {
                return bytes("value");
            }
            if (LIST_COMMANDS.contains(name)
                    || "LPOP".equals(name)
                    || "RPOP".equals(name)
                    || "SRANDMEMBER".equals(name)
                    || "SPOP".equals(name)
                    || "ZRANDMEMBER".equals(name)) {
                return List.of(1L);
            }
            if ("XADD".equals(name)) {
                return bytes("1-0");
            }
            if ("ZSCORE".equals(name) || "GEODIST".equals(name)) {
                return bytes("1.5");
            }
            return 1L;
        }

        private Set<String> commandNames() {
            java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
            calls.forEach(call -> names.add(String.valueOf(call.get(0))));
            return names;
        }
    }
}
