package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class FerricStoreIntegrationTest {
    @Test
    void kvAndFlowRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String suffix = suffix();

            assertTrue(client.kv().set("it:kv:" + suffix, Map.of("ok", true)));
            assertEquals(Map.of("ok", true), client.kv().get("it:kv:" + suffix));

            String id = "it-flow-" + suffix;
            String partition = "it-partition-" + suffix;
            client.create(
                    CreateOptions.builder(id, "it_order")
                            .state("created")
                            .partitionKey(partition)
                            .payload(Map.of("amount", 42))
                            .idempotent(true)
                            .build());

            ClaimedItem job = claimOne(client, "it_order", "created", partition, "it-worker");
            client.complete(
                    CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                            .partitionKey(partition)
                            .result(Map.of("ok", true))
                            .ttlMs(60_000)
                            .build());

            FlowRecord completed = client.get(id, partition);
            assertNotNull(completed);
            assertEquals("completed", completed.state());
        }
    }

    @Test
    void nativeHelpersAndDiagnosticsRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String suffix = suffix();
            String prefix = "java-sdk:native:" + suffix + ":";
            String key = prefix + "cas";
            String lockKey = prefix + "lock";
            String rateKey = prefix + "rate";
            String cacheKey = prefix + "cache";

            try {
                assertEquals("PONG", text(client.command("PING")));
                assertEquals("hello", text(client.command("ECHO", "hello")));
                assertTrue(ok(client.command("SET", key, client.codec().encode("old"))));
                assertTrue(client.cas(key, "old", "new", null));
                assertEquals("new", client.codec().decode((byte[]) client.command("GET", key)));

                assertTrue(client.lock(lockKey, "owner-a", 30_000));
                assertEquals(1, client.extendLock(lockKey, "owner-a", 30_000));
                assertEquals(1, client.unlock(lockKey, "owner-a"));

                RateLimitResult rate = client.ratelimitAdd(rateKey, 60_000, 5, 2);
                assertTrue(rate.count() >= 1);
                assertTrue(rate.remaining() >= 0);
                assertFalse(client.keyInfo(key).isEmpty());

                FetchOrComputeResult first = client.fetchOrCompute(cacheKey, 60_000, "integration");
                assertTrue(first.shouldCompute());
                assertTrue(client.fetchOrComputeResult(cacheKey, Map.of("computed", true), 60_000));
                FetchOrComputeResult cached = client.fetchOrCompute(cacheKey, 60_000, null);
                assertTrue(cached.hit());
                assertEquals(Map.of("computed", true), cached.value());
                assertTrue(client.fetchOrComputeError(prefix + "cache-error", "boom"));

                assertTrue(client.serverInfo("server").contains("#"));
                assertFalse(client.clusterHealth().isEmpty());
                assertFalse(client.clusterStats().isEmpty());
                assertTrue(client.clusterKeyslot(key) >= 0);
                assertNotNull(client.clusterSlots());
                assertFalse(client.clusterStatus().isEmpty());
                assertNotNull(client.clusterRole());
                assertNotNull(client.ferricstoreConfig("GET", "*"));
                assertFalse(client.ferricstoreMetrics().isEmpty());
                assertFalse(client.ferricstoreHotness().isEmpty());
                assertNotNull(client.ferricstoreDoctor("CHECK", "SCOPE", "BITCASK"));
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void rawStoreCommandFamiliesRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            String suffix = suffix();
            String prefix = "java-sdk:store:" + suffix + ":";

            try {
                assertStringCommands(client, prefix);
                assertHashCommands(client, prefix);
                assertListSetSortedSetCommands(client, prefix);
                assertStreamBitmapHllGeoCommands(client, prefix, suffix);
                assertProbabilisticCommands(client, prefix);
            } finally {
                deletePrefixedKeys(client, prefix);
            }
        }
    }

    @Test
    void flowStateMachineAndRepairSurfaceRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String suffix = suffix();
            String type = "java-sdk-flow-" + suffix;
            long now = System.currentTimeMillis();

            Object valueResponse =
                    client.valuePut(
                            Map.of("shared", true),
                            null,
                            null,
                            "java-sdk:value:" + suffix,
                            60_000L);
            assertNotNull(valueResponse);

            String signalId = "java-sdk:signal:" + suffix;
            String signalPartition = signalId + ":partition";
            client.create(
                    CreateOptions.builder(signalId, type)
                            .state("created")
                            .partitionKey(signalPartition)
                            .payload(Map.of("step", "created"))
                            .idempotent(true)
                            .build());
            assertNotNull(
                    client.signal(
                            signalId,
                            "approve",
                            "approved",
                            signalPartition,
                            Map.of(),
                            List.of("created")));
            FlowRecord signaled = client.get(signalId, signalPartition);
            assertNotNull(signaled);
            assertEquals("approved", signaled.state());

            assertBatchFlowCommands(client, type, suffix, now);
            assertSingleMutationCommands(client, type, suffix, now);
            assertManyMutationCommands(client, type, suffix, now);
            assertRepairIndexAndRewindCommands(client, type, suffix, now);

            assertFalse(client.list(type, null, null, 100).isEmpty());
            assertNotNull(client.flowInfo(type));
            assertFalse(client.history(signalId, signalPartition, 5).isEmpty());
            assertNotNull(client.retentionCleanup(10, null));
        }
    }

    @Test
    void queueAndWorkflowWrappersRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String suffix = suffix();

            String queueType = "java-sdk-queue-" + suffix;
            Queue queue = new QueueClient(client).queue(queueType);
            queue.enqueue("java-sdk:queue:" + suffix, Map.of("step", "queued"));
            QueueWorkerResult queueResult =
                    queue.worker("java-sdk-queue-worker")
                            .batchSize(1)
                            .runOnce(
                                    job -> {
                                        assertEquals(Map.of("step", "queued"), job.payload());
                                        return Map.of("ok", true);
                                    });
            assertEquals(new QueueWorkerResult(1, 1, 0, 0), queueResult);

            String workflowType = "java-sdk-workflow-" + suffix;
            Workflow workflow =
                    new WorkflowClient(client)
                            .workflow(workflowType, "received")
                            .state("received", ctx -> Outcomes.transition("validated"))
                            .state(
                                    "validated",
                                    ctx -> Outcomes.complete(Map.of("id", ctx.id(), "done", true)));
            String workflowId = "java-sdk:workflow:" + suffix;
            workflow.start(workflowId, Map.of("order", suffix));
            assertEquals(
                    1,
                    workflow.worker("java-sdk-workflow-worker", List.of("received"))
                            .batchSize(1)
                            .runOnce());
            assertEquals(
                    1,
                    workflow.worker("java-sdk-workflow-worker", List.of("validated"))
                            .batchSize(1)
                            .runOnce());
            FlowRecord completed = client.get(workflowId, null);
            assertNotNull(completed);
            assertEquals("completed", completed.state());
        }
    }

    private static void assertStringCommands(FerricStoreClient client, String prefix) {
        String key = prefix + "string";
        assertTrue(client.kv().set(key, "abc", 60_000L, null));
        assertEquals("abc", client.kv().get(key, String.class));
        assertEquals(1, client.kv().exists(key));
        assertEquals("abc", text(client.kv().mget(List.of(key, prefix + "missing")).getFirst()));
        assertTrue(ok(client.command("MSET", prefix + "string2", "2", prefix + "string3", "3")));
        assertEquals(1, number(client.command("MSETNX", prefix + "nx1", "1", prefix + "nx2", "2")));
        assertEquals(1, client.kv().incr(prefix + "counter"));
        assertEquals(5, client.kv().incrBy(prefix + "counter", 4));
        assertEquals(4, client.kv().decr(prefix + "counter"));
        assertEquals(2, client.kv().decrBy(prefix + "counter", 2));
        assertTrue(
                Double.parseDouble(text(client.command("INCRBYFLOAT", prefix + "float", "1.5")))
                        >= 1.5);
        assertEquals(3, number(client.command("APPEND", prefix + "append", "abc")));
        assertEquals(3, number(client.command("STRLEN", prefix + "append")));
        assertEquals("abc", text(client.command("GETSET", prefix + "append", "xyz")));
        assertEquals("xy", text(client.command("GETRANGE", prefix + "append", 0, 1)));
        assertEquals(3, number(client.command("SETRANGE", prefix + "append", 1, "Q")));
        assertEquals("xQz", text(client.command("GETEX", prefix + "append", "PX", 60_000)));
        assertTrue(client.kv().ttl(prefix + "append") >= 0);
        assertTrue(number(client.command("PTTL", prefix + "append")) >= 0);
        assertEquals(1, number(client.command("PERSIST", prefix + "append")));
        assertTrue(client.kv().expire(prefix + "append", 60));
        assertEquals(1, number(client.command("PEXPIRE", prefix + "append", 60_000)));
        assertEquals(
                1,
                number(
                        client.command(
                                "EXPIREAT",
                                prefix + "append",
                                System.currentTimeMillis() / 1000 + 60)));
        assertEquals(
                1,
                number(
                        client.command(
                                "PEXPIREAT",
                                prefix + "append",
                                System.currentTimeMillis() + 60_000)));
        assertTrue(number(client.command("EXPIRETIME", prefix + "append")) >= 0);
        assertTrue(number(client.command("PEXPIRETIME", prefix + "append")) >= 0);
        assertEquals("string", client.kv().type(prefix + "append"));
        assertEquals(1, number(client.command("SETNX", prefix + "setnx", "1")));
        assertTrue(ok(client.command("SETEX", prefix + "setex", 60, "1")));
        assertTrue(ok(client.command("PSETEX", prefix + "psetex", 60_000, "1")));
        assertEquals(1, number(client.command("COPY", key, prefix + "copy", "REPLACE")));
        assertTrue(ok(client.command("RENAME", prefix + "copy", prefix + "renamed")));
        assertEquals(
                1, number(client.command("RENAMENX", prefix + "renamed", prefix + "renamed-nx")));
        assertNotNull(client.command("RANDOMKEY"));
        assertFalse(client.kv().keys(prefix + "*").isEmpty());
        assertNotNull(client.kv().scan("0", prefix + "*", 10L));
        assertTrue(number(client.command("DBSIZE")) >= 1);
        assertNotNull(client.command("OBJECT", "ENCODING", key));
        assertFalse(list(client.command("OBJECT", "HELP")).isEmpty());
        assertTrue(number(client.command("OBJECT", "FREQ", key)) >= 0);
        assertTrue(number(client.command("OBJECT", "IDLETIME", key)) >= 0);
        assertEquals(1, number(client.command("OBJECT", "REFCOUNT", key)));
        assertEquals(0, number(client.command("WAIT", 0, 1)));
        assertNotNull(client.command("WAITAOF", 0, 0, 1));
        assertTrue(number(client.command("MEMORY", "USAGE", key)) >= 0);
        assertEquals("1", text(client.command("GETDEL", prefix + "setnx")));
        assertTrue(number(client.command("UNLINK", prefix + "nx1")) >= 0);
    }

    private static void assertHashCommands(FerricStoreClient client, String prefix) {
        String key = prefix + "hash";
        assertTrue(client.hash().hset(key, Map.of("field", "value", "count", "1")) >= 1);
        assertEquals("value", text(client.hash().hget(key, "field")));
        assertEquals("value", text(list(client.command("HMGET", key, "field", "none")).getFirst()));
        assertNotNull(client.hash().hgetall(key));
        assertTrue(client.hash().hexists(key, "field"));
        assertFalse(client.hash().hkeys(key).isEmpty());
        assertFalse(list(client.command("HVALS", key)).isEmpty());
        assertTrue(client.hash().hlen(key) >= 2);
        assertEquals(3, client.hash().hincrBy(key, "count", 2));
        assertTrue(
                Double.parseDouble(text(client.command("HINCRBYFLOAT", key, "float", "1.25")))
                        >= 1.25);
        assertEquals(1, number(client.command("HSETNX", key, "new", "item")));
        assertEquals(5, number(client.command("HSTRLEN", key, "field")));
        assertNotNull(client.command("HRANDFIELD", key, 1, "WITHVALUES"));
        assertNotNull(client.command("HSCAN", key, 0, "COUNT", 10));
        assertNotNull(client.command("HEXPIRE", key, 60, "FIELDS", 1, "field"));
        assertNotNull(client.command("HTTL", key, "FIELDS", 1, "field"));
        assertNotNull(client.command("HPERSIST", key, "FIELDS", 1, "field"));
        assertNotNull(client.command("HPEXPIRE", key, 60_000, "FIELDS", 1, "field"));
        assertNotNull(client.command("HPTTL", key, "FIELDS", 1, "field"));
        assertNotNull(client.command("HEXPIRETIME", key, "FIELDS", 1, "field"));
        assertEquals(
                "value",
                text(
                        list(client.command("HGETEX", key, "PX", 60_000, "FIELDS", 1, "field"))
                                .getFirst()));
        assertTrue(number(client.command("HSETEX", key, 60, "temp", "1")) >= 0);
        assertEquals(
                "1", text(list(client.command("HGETDEL", key, "FIELDS", 1, "temp")).getFirst()));
        assertEquals(1, client.hash().hdel(key, "new"));
    }

    private static void assertListSetSortedSetCommands(FerricStoreClient client, String prefix) {
        assertListCommands(client, prefix);
        assertSetAndSortedSetCommands(client, prefix);
    }

    private static void assertListCommands(FerricStoreClient client, String prefix) {
        String listKey = prefix + "list";
        String listDst = prefix + "list-dst";
        assertEquals(2, client.lists().lpush(listKey, "b", "a"));
        assertEquals(3, client.lists().rpush(listKey, "c"));
        assertFalse(client.lists().lrange(listKey, 0, -1).isEmpty());
        assertEquals(3, client.lists().llen(listKey));
        assertEquals("a", text(client.command("LINDEX", listKey, 0)));
        assertTrue(ok(client.command("LSET", listKey, 1, "bb")));
        assertEquals(1, number(client.command("LREM", listKey, 0, "bb")));
        assertTrue(ok(client.command("LTRIM", listKey, 0, 1)));
        assertEquals(0, number(client.command("LPOS", listKey, "a")));
        assertTrue(number(client.command("LINSERT", listKey, "AFTER", "a", "aa")) >= 0);
        assertNotNull(client.command("LMOVE", listKey, listDst, "LEFT", "RIGHT"));
        assertNotNull(client.command("RPOPLPUSH", listDst, listKey));
        assertTrue(number(client.command("LPUSHX", listKey, "left")) >= 1);
        assertTrue(number(client.command("RPUSHX", listKey, "right")) >= 1);
        assertNotNull(client.lists().blpop(List.of(listKey), 1));
        assertTrue(client.lists().rpush(listKey, "block") >= 1);
        assertNotNull(client.command("BRPOP", listKey, 1));
        assertTrue(client.lists().rpush(listKey, "move") >= 1);
        assertNotNull(client.command("BLMOVE", listKey, listDst, "LEFT", "RIGHT", 1));
        assertTrue(client.lists().rpush(listKey, "mpop") >= 1);
        assertNotNull(client.command("BLMPOP", 1, 1, listKey, "LEFT", "COUNT", 1));
    }

    private static void assertSetAndSortedSetCommands(FerricStoreClient client, String prefix) {
        String setA = prefix + "set-a";
        String setB = prefix + "set-b";
        assertEquals(2, client.sets().sadd(setA, "a", "b"));
        assertEquals(2, client.sets().sadd(setB, "b", "c"));
        assertTrue(client.sets().sismember(setA, "a"));
        assertFalse(client.sets().smembers(setA).isEmpty());
        assertNotNull(client.command("SMISMEMBER", setA, "a", "z"));
        assertEquals(2, number(client.command("SCARD", setA)));
        assertNotNull(client.command("SRANDMEMBER", setA, 1));
        assertNotNull(client.command("SDIFF", setA, setB));
        assertNotNull(client.command("SINTER", setA, setB));
        assertNotNull(client.command("SUNION", setA, setB));
        assertTrue(number(client.command("SDIFFSTORE", prefix + "sdiff", setA, setB)) >= 0);
        assertTrue(number(client.command("SINTERSTORE", prefix + "sinter", setA, setB)) >= 0);
        assertTrue(number(client.command("SUNIONSTORE", prefix + "sunion", setA, setB)) >= 0);
        assertTrue(number(client.command("SINTERCARD", 2, setA, setB, "LIMIT", 10)) >= 0);
        assertTrue(number(client.command("SMOVE", setA, setB, "a")) >= 0);
        assertNotNull(client.command("SSCAN", setB, 0, "COUNT", 10));
        assertNotNull(client.command("SPOP", setB, 1));
        assertTrue(client.sets().srem(setA, "b") >= 0);

        String zset = prefix + "zset";
        assertEquals(
                3,
                client.zset()
                        .zadd(
                                zset,
                                List.of(
                                        new ZAddMember(1, "a"),
                                        new ZAddMember(2, "b"),
                                        new ZAddMember(3, "c"))));
        assertNotNull(client.zset().zscore(zset, "a"));
        assertEquals(0, number(client.command("ZRANK", zset, "a")));
        assertEquals(0, number(client.command("ZREVRANK", zset, "c")));
        assertFalse(client.zset().zrange(zset, 0, -1).isEmpty());
        assertNotNull(client.command("ZREVRANGE", zset, 0, -1));
        assertEquals(3, number(client.command("ZCARD", zset)));
        assertNotNull(client.command("ZINCRBY", zset, 1, "a"));
        assertTrue(number(client.command("ZCOUNT", zset, "-inf", "+inf")) >= 3);
        assertNotNull(client.command("ZRANDMEMBER", zset, 1, "WITHSCORES"));
        assertNotNull(client.command("ZMSCORE", zset, "a", "none"));
        assertNotNull(client.command("ZRANGEBYSCORE", zset, "-inf", "+inf"));
        assertNotNull(client.command("ZREVRANGEBYSCORE", zset, "+inf", "-inf"));
        assertNotNull(client.command("ZSCAN", zset, 0, "COUNT", 10));
        assertEquals(1, client.zset().zrem(zset, "b"));
        assertNotNull(client.command("ZPOPMIN", zset, 1));
        assertNotNull(client.command("ZPOPMAX", zset, 1));
    }

    private static void assertStreamBitmapHllGeoCommands(
            FerricStoreClient client, String prefix, String suffix) {
        String stream = prefix + "stream";
        String streamId = text(client.stream().xadd(stream, "*", Map.of("field", "value")));
        assertTrue(client.stream().xlen(stream) >= 1);
        assertFalse(client.stream().xrange(stream, "-", "+").isEmpty());
        assertNotNull(client.command("XREVRANGE", stream, "+", "-"));
        assertNotNull(client.command("XREAD", "COUNT", 1, "STREAMS", stream, "0-0"));
        assertNotNull(client.command("XINFO", "STREAM", stream));
        String group = "group-" + suffix;
        assertTrue(ok(client.command("XGROUP", "CREATE", stream, group, "0")));
        assertNotNull(
                client.command(
                        "XREADGROUP",
                        "GROUP",
                        group,
                        "consumer",
                        "COUNT",
                        1,
                        "STREAMS",
                        stream,
                        ">"));
        assertTrue(client.stream().xack(stream, group, streamId) >= 0);
        assertTrue(number(client.command("XTRIM", stream, "MAXLEN", "~", 10)) >= 0);
        assertTrue(number(client.command("XDEL", stream, streamId)) >= 0);

        String bitmap = prefix + "bitmap";
        assertEquals(0, client.bitmap().setbit(bitmap, 7, 1));
        assertEquals(1, client.bitmap().getbit(bitmap, 7));
        assertTrue(client.bitmap().bitcount(bitmap) >= 1);
        assertTrue(number(client.command("BITPOS", bitmap, 1)) >= 0);
        assertTrue(number(client.command("BITOP", "OR", prefix + "bitmap-out", bitmap)) >= 1);

        String hll = prefix + "hll";
        assertTrue(client.hyperloglog().pfadd(hll, "a", "b") >= 0);
        assertTrue(client.hyperloglog().pfcount(hll) >= 1);
        assertTrue(ok(client.command("PFMERGE", prefix + "hll-dst", hll)));

        String geo = prefix + "geo";
        assertEquals(
                1,
                client.geo().geoadd(geo, List.of(new GeoMember(13.361389, 38.115556, "palermo"))));
        assertEquals(
                1,
                client.geo().geoadd(geo, List.of(new GeoMember(15.087269, 37.502669, "catania"))));
        assertNotNull(client.geo().geopos(geo, "palermo"));
        assertNotNull(client.command("GEODIST", geo, "palermo", "catania", "km"));
        assertNotNull(client.command("GEOHASH", geo, "palermo"));
        assertNotNull(
                client.command("GEOSEARCH", geo, "FROMMEMBER", "palermo", "BYRADIUS", 200, "km"));
        assertTrue(
                number(
                                client.command(
                                        "GEOSEARCHSTORE",
                                        prefix + "geo-dst",
                                        geo,
                                        "FROMMEMBER",
                                        "palermo",
                                        "BYRADIUS",
                                        200,
                                        "km"))
                        >= 0);
    }

    private static void assertProbabilisticCommands(FerricStoreClient client, String prefix) {
        String bloom = prefix + "bf";
        assertTrue(client.bloom().reserve(bloom, 0.01, 100));
        assertInstanceOf(Boolean.class, client.bloom().add(bloom, "a"));
        assertEquals(2, client.bloom().madd(bloom, "b", "c").size());
        assertInstanceOf(Boolean.class, client.bloom().exists(bloom, "a"));
        assertNotNull(client.command("BF.MEXISTS", bloom, "a", "z"));
        assertTrue(client.bloom().card(bloom) >= 1);
        assertNotNull(client.bloom().info(bloom));

        String cuckoo = prefix + "cf";
        assertTrue(client.cuckoo().reserve(cuckoo, 100));
        assertInstanceOf(Boolean.class, client.cuckoo().add(cuckoo, "a"));
        assertInstanceOf(Boolean.class, client.cuckoo().addnx(cuckoo, "b"));
        assertInstanceOf(Boolean.class, client.cuckoo().exists(cuckoo, "a"));
        assertNotNull(client.command("CF.MEXISTS", cuckoo, "a", "z"));
        assertTrue(client.cuckoo().count(cuckoo, "a") >= 0);
        assertInstanceOf(Boolean.class, client.cuckoo().del(cuckoo, "a"));
        assertNotNull(client.cuckoo().info(cuckoo));

        String cmsA = prefix + "cms-a";
        String cmsB = prefix + "cms-b";
        String cmsProb = prefix + "cms-prob";
        assertTrue(client.cms().initByDim(cmsA, 20, 4));
        assertTrue(client.cms().initByDim(cmsB, 20, 4));
        assertTrue(client.cms().initByProb(cmsProb, 0.01, 0.01));
        assertFalse(client.cms().incrBy(cmsA, "a", 2).isEmpty());
        assertFalse(client.cms().incrBy(cmsB, "a", 1).isEmpty());
        assertEquals(2, client.cms().query(cmsA, "a", "b").size());
        assertTrue(ok(client.command("CMS.MERGE", prefix + "cms-dst", 2, cmsA, cmsB)));
        assertNotNull(client.cms().info(prefix + "cms-dst"));

        String topk = prefix + "topk";
        assertTrue(client.topk().reserve(topk, 3));
        assertEquals(3, client.topk().add(topk, "a", "b", "a").size());
        assertNotNull(client.command("TOPK.INCRBY", topk, "c", 2));
        assertEquals(2, client.topk().query(topk, "a", "z").size());
        assertNotNull(client.command("TOPK.LIST", topk, "WITHCOUNT"));
        assertNotNull(client.command("TOPK.COUNT", topk, "a", "z"));
        assertNotNull(client.topk().info(topk));

        String tdigest = prefix + "tdigest";
        String tdigestSrc = prefix + "tdigest-src";
        assertTrue(client.tdigest().create(tdigest));
        assertTrue(client.tdigest().add(tdigest, 1, 2, 3, 4));
        assertEquals(1, client.tdigest().quantile(tdigest, 0.5).size());
        assertNotNull(client.command("TDIGEST.CDF", tdigest, 2));
        assertNotNull(client.command("TDIGEST.RANK", tdigest, 2));
        assertNotNull(client.command("TDIGEST.REVRANK", tdigest, 2));
        assertNotNull(client.command("TDIGEST.BYRANK", tdigest, 1));
        assertNotNull(client.command("TDIGEST.BYREVRANK", tdigest, 1));
        assertNotNull(client.command("TDIGEST.TRIMMED_MEAN", tdigest, "0.1", "0.9"));
        assertNotNull(client.command("TDIGEST.MIN", tdigest));
        assertNotNull(client.command("TDIGEST.MAX", tdigest));
        assertNotNull(client.tdigest().info(tdigest));
        assertTrue(client.tdigest().create(tdigestSrc));
        assertTrue(client.tdigest().add(tdigestSrc, 5, 6));
        assertTrue(
                ok(
                        client.command(
                                "TDIGEST.MERGE",
                                prefix + "tdigest-dst",
                                2,
                                tdigest,
                                tdigestSrc,
                                "OVERRIDE")));
        assertTrue(ok(client.command("TDIGEST.RESET", tdigest)));
    }

    private static void assertBatchFlowCommands(
            FerricStoreClient client, String type, String suffix, long now) {
        String partition = "java-sdk:batch:" + suffix + ":partition";
        client.createMany(
                CreateManyOptions.builder(
                                type,
                                List.of(
                                        new CreateItem(
                                                "java-sdk:batch:" + suffix + ":a", Map.of("n", 1)),
                                        new CreateItem(
                                                "java-sdk:batch:" + suffix + ":b", Map.of("n", 2))))
                        .partitionKey(partition)
                        .state("batch")
                        .nowMs(now)
                        .runAtMs(now)
                        .idempotent(true)
                        .build());
        List<ClaimedItem> jobs =
                client.claimJobs(
                        ClaimDueOptions.builder(type, "java-sdk-batch-worker")
                                .state("batch")
                                .partitionKey(partition)
                                .limit(2)
                                .nowMs(now)
                                .build());
        assertEquals(2, jobs.size());
    }

    private static void assertSingleMutationCommands(
            FerricStoreClient client, String type, String suffix, long now) {
        ClaimedFlow transition =
                createAndClaim(client, type, suffix, "transition", "queued", now, 30_000);
        assertNotNull(
                client.extendLease(
                        transition.job().id(),
                        transition.job().leaseToken(),
                        transition.job().fencingToken(),
                        30_000,
                        transition.partitionKey()));
        assertNotNull(
                client.transition(
                        TransitionOptions.builder(
                                        transition.id(),
                                        transition.job().state(),
                                        "ready",
                                        transition.job().leaseToken(),
                                        transition.job().fencingToken())
                                .partitionKey(transition.partitionKey())
                                .payload(Map.of("step", "ready"))
                                .build()));
        ClaimedItem ready =
                claimOne(client, type, "ready", transition.partitionKey(), "java-sdk-ready-worker");
        assertNotNull(
                client.complete(
                        CompleteOptions.builder(
                                        ready.id(), ready.leaseToken(), ready.fencingToken())
                                .partitionKey(ready.partitionKey())
                                .result(Map.of("ok", true))
                                .build()));

        ClaimedFlow retry = createAndClaim(client, type, suffix, "retry", "queued", now, 30_000);
        assertNotNull(
                client.retry(
                        RetryOptions.builder(
                                        retry.id(),
                                        retry.job().leaseToken(),
                                        retry.job().fencingToken())
                                .partitionKey(retry.partitionKey())
                                .error(Map.of("retry", true))
                                .runAtMs(now)
                                .nowMs(now)
                                .build()));
        ClaimedItem retried =
                claimOne(client, type, "queued", retry.partitionKey(), "java-sdk-retry-worker");
        assertNotNull(
                client.complete(
                        CompleteOptions.builder(
                                        retried.id(), retried.leaseToken(), retried.fencingToken())
                                .partitionKey(retried.partitionKey())
                                .build()));

        ClaimedFlow failed = createAndClaim(client, type, suffix, "fail", "queued", now, 30_000);
        assertNotNull(
                client.fail(
                        FailOptions.builder(
                                        failed.id(),
                                        failed.job().leaseToken(),
                                        failed.job().fencingToken())
                                .partitionKey(failed.partitionKey())
                                .error(Map.of("failed", true))
                                .build()));
        assertEquals("failed", client.get(failed.id(), failed.partitionKey()).state());
        assertNotNull(client.failures(type, null, 20));

        ClaimedFlow cancelled =
                createAndClaim(client, type, suffix, "cancel", "queued", now, 30_000);
        assertNotNull(
                client.cancel(
                        CancelOptions.builder(cancelled.id(), cancelled.job().fencingToken())
                                .leaseToken(cancelled.job().leaseToken())
                                .partitionKey(cancelled.partitionKey())
                                .reason(Map.of("cancelled", true))
                                .build()));
        assertEquals("cancelled", client.get(cancelled.id(), cancelled.partitionKey()).state());
        assertNotNull(client.terminals(type, null, null, 50));
    }

    private static void assertManyMutationCommands(
            FerricStoreClient client, String type, String suffix, long now) {
        String transitionPartition = "java-sdk:many:" + suffix + ":partition";
        client.createMany(
                CreateManyOptions.builder(
                                type,
                                List.of(
                                        new CreateItem("java-sdk:many:" + suffix + ":a", Map.of()),
                                        new CreateItem("java-sdk:many:" + suffix + ":b", Map.of())))
                        .partitionKey(transitionPartition)
                        .state("many-transition")
                        .nowMs(now)
                        .runAtMs(now)
                        .build());
        List<ClaimedItem> manyJobs =
                claimMany(client, type, "many-transition", transitionPartition, now, 2);
        assertNotNull(
                client.transitionMany(
                        TransitionManyOptions.builder(
                                        manyJobs.getFirst().state(),
                                        "many-complete",
                                        manyJobs.stream()
                                                .map(FerricStoreIntegrationTest::fenced)
                                                .toList())
                                .partitionKey(transitionPartition)
                                .nowMs(now)
                                .build()));
        List<ClaimedItem> completeJobs =
                claimMany(client, type, "many-complete", transitionPartition, now + 1, 2);
        assertEquals(2, completeJobs.size());

        String retryPartition = "java-sdk:retry-many:" + suffix + ":partition";
        createManyState(client, type, retryPartition, "retry-many", suffix, "retry-many", now);
        List<ClaimedItem> retryJobs = claimMany(client, type, "retry-many", retryPartition, now, 2);
        assertNotNull(
                client.retryMany(
                        RetryManyOptions.builder(retryJobs)
                                .partitionKey(retryPartition)
                                .error(Map.of("retry", "many"))
                                .runAtMs(now)
                                .nowMs(now)
                                .build()));
        List<ClaimedItem> retryAgain =
                claimMany(client, type, "retry-many", retryPartition, now + 1, 2);
        assertNotNull(
                client.failMany(
                        FailManyOptions.builder(retryAgain)
                                .partitionKey(retryPartition)
                                .error(Map.of("done", true))
                                .build()));

        String cancelPartition = "java-sdk:cancel-many:" + suffix + ":partition";
        createManyState(client, type, cancelPartition, "cancel-many", suffix, "cancel-many", now);
        List<ClaimedItem> cancelJobs =
                claimMany(client, type, "cancel-many", cancelPartition, now, 2);
        assertNotNull(
                client.cancelMany(
                        CancelManyOptions.builder(
                                        cancelJobs.stream()
                                                .map(FerricStoreIntegrationTest::fenced)
                                                .toList())
                                .partitionKey(cancelPartition)
                                .reason(Map.of("cancel", "many"))
                                .build()));
    }

    private static void assertRepairIndexAndRewindCommands(
            FerricStoreClient client, String type, String suffix, long now) {
        String reclaimId = "java-sdk:reclaim:" + suffix;
        String reclaimPartition = reclaimId + ":partition";
        client.create(
                CreateOptions.builder(reclaimId, type)
                        .state("reclaim")
                        .partitionKey(reclaimPartition)
                        .nowMs(1_000)
                        .runAtMs(1_000)
                        .build());
        claimOneAt(
                client, type, "reclaim", reclaimPartition, "java-sdk-reclaim-initial", 1_000, 10);
        List<ClaimedItem> reclaimed =
                client.reclaimJobs(
                        ClaimDueOptions.builder(type, "java-sdk-reclaim-worker")
                                .partitionKey(reclaimPartition)
                                .limit(1)
                                .nowMs(2_000)
                                .leaseMs(30_000)
                                .build());
        assertEquals(1, reclaimed.size());
        ClaimedItem reclaimedJob = reclaimed.getFirst();
        assertNotNull(
                client.complete(
                        CompleteOptions.builder(
                                        reclaimedJob.id(),
                                        reclaimedJob.leaseToken(),
                                        reclaimedJob.fencingToken())
                                .partitionKey(reclaimedJob.partitionKey())
                                .build()));

        String stuckId = "java-sdk:stuck:" + suffix;
        String stuckPartition = stuckId + ":partition";
        client.create(
                CreateOptions.builder(stuckId, type)
                        .state("stuck")
                        .partitionKey(stuckPartition)
                        .nowMs(1_000)
                        .runAtMs(1_000)
                        .build());
        ClaimedItem stuck =
                claimOneAt(
                        client,
                        type,
                        "stuck",
                        stuckPartition,
                        "java-sdk-stuck-worker",
                        1_000,
                        60_000);
        assertTrue(
                client.stuck(type, stuckPartition, 10, 1L, 120_000L).stream()
                        .anyMatch(record -> record.id().equals(stuckId)));
        assertNotNull(
                client.complete(
                        CompleteOptions.builder(
                                        stuck.id(), stuck.leaseToken(), stuck.fencingToken())
                                .partitionKey(stuck.partitionKey())
                                .build()));

        String parentId = "java-sdk:parent:" + suffix;
        String parentPartition = parentId + ":partition";
        client.create(
                CreateOptions.builder(parentId, type)
                        .state("dispatch")
                        .partitionKey(parentPartition)
                        .rootFlowId("root:" + suffix)
                        .correlationId("corr:" + suffix)
                        .idempotent(true)
                        .build());
        FlowRecord parent = client.get(parentId, parentPartition);
        assertNotNull(parent);
        assertNotNull(
                client.spawnChildren(
                        SpawnChildrenOptions.builder(
                                        parentId,
                                        List.of(
                                                new ChildSpec(
                                                        "java-sdk:child:" + suffix + ":a",
                                                        type,
                                                        Map.of("child", "a")),
                                                new ChildSpec(
                                                        "java-sdk:child:" + suffix + ":b",
                                                        type,
                                                        Map.of("child", "b"))))
                                .partitionKey(parentPartition)
                                .fencingToken(parent.fencingToken())
                                .groupId("fanout")
                                .waitMode("any")
                                .fromState("dispatch")
                                .waitState("waiting_children")
                                .success("children_done")
                                .failure("children_failed")
                                .build()));
        assertTrue(
                client.byParent(parentId, null, 20).stream()
                        .anyMatch(
                                record ->
                                        record.id().startsWith("java-sdk:child:" + suffix + ":")));
        assertTrue(
                client.byRoot("root:" + suffix, null, 20).stream()
                        .anyMatch(record -> record.id().equals(parentId)));
        assertTrue(
                client.byCorrelation("corr:" + suffix, null, 20).stream()
                        .anyMatch(record -> record.id().equals(parentId)));

        ClaimedFlow rewind = createAndClaim(client, type, suffix, "rewind", "queued", now, 30_000);
        String createdEventId =
                eventId(client.history(rewind.id(), rewind.partitionKey(), 10).getFirst());
        client.complete(
                CompleteOptions.builder(
                                rewind.job().id(),
                                rewind.job().leaseToken(),
                                rewind.job().fencingToken())
                        .partitionKey(rewind.job().partitionKey())
                        .build());
        FlowRecord rewound =
                (FlowRecord)
                        client.rewind(
                                rewind.id(),
                                createdEventId,
                                rewind.partitionKey(),
                                "completed",
                                now,
                                null,
                                null,
                                true);
        assertEquals("queued", rewound.state());
    }

    private static void createManyState(
            FerricStoreClient client,
            String type,
            String partition,
            String state,
            String suffix,
            String name,
            long now) {
        client.createMany(
                CreateManyOptions.builder(
                                type,
                                List.of(
                                        new CreateItem(
                                                "java-sdk:" + name + ":" + suffix + ":a", Map.of()),
                                        new CreateItem(
                                                "java-sdk:" + name + ":" + suffix + ":b",
                                                Map.of())))
                        .partitionKey(partition)
                        .state(state)
                        .nowMs(now)
                        .runAtMs(now)
                        .build());
    }

    private static ClaimedFlow createAndClaim(
            FerricStoreClient client,
            String type,
            String suffix,
            String name,
            String state,
            long now,
            long leaseMs) {
        String id = "java-sdk:" + name + ":" + suffix;
        String partition = id + ":partition";
        client.create(
                CreateOptions.builder(id, type)
                        .state(state)
                        .partitionKey(partition)
                        .payload(Map.of("name", name))
                        .nowMs(now)
                        .runAtMs(now)
                        .idempotent(true)
                        .build());
        return new ClaimedFlow(
                id,
                partition,
                claimOneAt(
                        client,
                        type,
                        state,
                        partition,
                        "java-sdk-" + name + "-worker",
                        now,
                        leaseMs));
    }

    private static List<ClaimedItem> claimMany(
            FerricStoreClient client,
            String type,
            String state,
            String partition,
            long now,
            int limit) {
        List<ClaimedItem> jobs =
                client.claimJobs(
                        ClaimDueOptions.builder(type, "java-sdk-many-worker")
                                .state(state)
                                .partitionKey(partition)
                                .limit(limit)
                                .nowMs(now)
                                .build());
        assertEquals(limit, jobs.size());
        return jobs;
    }

    private static ClaimedItem claimOne(
            FerricStoreClient client, String type, String state, String partition, String worker) {
        return claimOneAt(client, type, state, partition, worker, 0, 30_000);
    }

    private static ClaimedItem claimOneAt(
            FerricStoreClient client,
            String type,
            String state,
            String partition,
            String worker,
            long now,
            long leaseMs) {
        ClaimDueOptions.Builder builder =
                ClaimDueOptions.builder(type, worker)
                        .state(state)
                        .partitionKey(partition)
                        .limit(1)
                        .leaseMs(leaseMs);
        if (now > 0) {
            builder.nowMs(now);
        }
        List<ClaimedItem> jobs = client.claimJobs(builder.build());
        assertEquals(1, jobs.size());
        return jobs.getFirst();
    }

    private static FencedItem fenced(ClaimedItem job) {
        return new FencedItem(job.id(), job.fencingToken(), job.leaseToken(), job.partitionKey());
    }

    private static String eventId(Object event) {
        if (event instanceof List<?> list && !list.isEmpty()) {
            return text(list.getFirst());
        }
        Object value = field(event, "event_id");
        if (value == null) {
            value = field(event, "id");
        }
        assertNotNull(value);
        return text(value);
    }

    private static Object field(Object source, String name) {
        if (source instanceof Map<?, ?> map) {
            Object value = map.get(name);
            return value == null ? map.get(name.getBytes(StandardCharsets.UTF_8)) : value;
        }
        return null;
    }

    private static List<Object> list(Object value) {
        return Resp.list(value);
    }

    private static long number(Object value) {
        return Resp.number(value);
    }

    private static boolean ok(Object value) {
        return CommandArgs.ok(value) || numberOrMinusOne(value) == 1;
    }

    private static long numberOrMinusOne(Object value) {
        try {
            return number(value);
        } catch (RuntimeException error) {
            return -1;
        }
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Resp.string(value);
    }

    private static void deletePrefixedKeys(FerricStoreClient client, String prefix) {
        List<Object> keys = client.kv().keys(prefix + "*");
        if (!keys.isEmpty()) {
            List<Object> command = new ArrayList<>();
            command.add("DEL");
            command.addAll(keys);
            client.command(command);
        }
    }

    private static FerricStoreClient connectJson() {
        return FerricStoreClient.connect(
                System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0"),
                new JsonCodec());
    }

    private static FerricStoreClient connectRaw() {
        return FerricStoreClient.connect(
                System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0"),
                new RawCodec());
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static void assumeIntegration() {
        assumeTrue(
                "1".equals(System.getenv("FERRICSTORE_INTEGRATION")),
                "set FERRICSTORE_INTEGRATION=1 to run local FerricStore integration tests");
    }

    private record ClaimedFlow(String id, String partitionKey, ClaimedItem job) {}
}
