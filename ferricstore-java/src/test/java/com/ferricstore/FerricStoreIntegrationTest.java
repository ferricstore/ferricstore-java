package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
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

                boolean lockAcquired = client.lock(lockKey, "owner-a", 30_000);
                if (!lockAcquired) {
                    throw new AssertionError("expected integration lock acquisition to succeed");
                }
                try {
                    assertEquals(1, client.extendLock(lockKey, "owner-a", 30_000));
                } finally {
                    long unlockResult = client.unlock(lockKey, "owner-a");
                    assertEquals(1, unlockResult);
                }

                RateLimitResult rate = client.ratelimitAdd(rateKey, 60_000, 5, 2);
                assertTrue(rate.count() >= 1);
                assertTrue(rate.remaining() >= 0);
                assertFalse(client.keyInfo(key).isEmpty());

                if (!isHttpIntegration()) {
                    FetchOrComputeResult first =
                            client.fetchOrCompute(cacheKey, 60_000, "integration");
                    assertTrue(first.shouldCompute());
                    assertTrue(
                            client.fetchOrComputeResult(
                                    cacheKey,
                                    first.ownershipToken(),
                                    Map.of("computed", true),
                                    60_000));
                    FetchOrComputeResult cached = client.fetchOrCompute(cacheKey, 60_000, null);
                    assertTrue(cached.hit());
                    assertEquals(Map.of("computed", true), cached.value());
                    FetchOrComputeResult failed =
                            client.fetchOrCompute(prefix + "cache-error", 60_000, "integration");
                    assertTrue(
                            client.fetchOrComputeError(
                                    prefix + "cache-error", failed.ownershipToken(), "boom"));
                }

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
            String prefix = "{java-sdk:store:" + suffix + "}:";

            try {
                assertStringCommands(client, prefix);
                assertHashCommands(client, prefix);
                assertListSetSortedSetCommands(client, prefix);
                assertStreamBitmapHllGeoCommands(client, prefix, suffix);
                assertProbabilisticCommands(client, prefix);
                assertJsonDocuments(client, prefix);
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
            Object valueRef = field(valueResponse, "ref");
            assertNotNull(valueRef);
            assertEquals(
                    List.of(Map.of("shared", true)), client.valueMGet(List.of(text(valueRef))));

            String signalId = "java-sdk:signal:" + suffix;
            String signalPartition = signalId + ":partition";
            client.create(
                    CreateOptions.builder(signalId, type)
                            .state("created")
                            .partitionKey(signalPartition)
                            .payload(Map.of("step", "created"))
                            .attribute("tenant", "acme")
                            .stateMeta("source", "java-sdk")
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
            assertEquals("acme", text(signaled.attributes().get("tenant")));
            assertNotNull(signaled.stateMeta().get("created"));

            assertBatchFlowCommands(client, type, suffix, now);
            assertSingleMutationCommands(client, type, suffix, now);
            assertManyMutationCommands(client, type, suffix, now);
            assertRepairIndexAndRewindCommands(client, type, suffix, now);

            assertEventuallyContains(
                    () -> client.list(type, "approved", signalPartition, 100), signalId);
            assertNotNull(client.flowInfo(type));
            assertFalse(
                    client.history(
                                    signalId,
                                    HistoryOptions.builder()
                                            .partitionKey(signalPartition)
                                            .count(5)
                                            .values(true)
                                            .payloadMaxBytes(65_536)
                                            .build())
                            .isEmpty());
            assertNotNull(client.retentionCleanup(10, null));
        }
    }

    @Test
    void flowAdministrationAndGovernanceRoundTripAgainstLocalServer() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String suffix = suffix();
            String type = "java-sdk-admin-" + suffix;
            String partition = "java-sdk:admin:" + suffix + ":partition";
            long now = System.currentTimeMillis();

            assertFlowInsights(client, type, partition, suffix, now);
            assertFlowSteps(client, type, partition, suffix, now);
            assertFlowSchedules(client, type, partition, suffix, now);
            assertFlowGovernance(client, type, suffix, now);
            assertNotNull(client.flowInsights().queryIndexes());
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

    @Test
    void nativeTransactionsAndPubSubUseIsolatedPersistentConnections() {
        assumeIntegration();
        assumeTrue(!isHttpIntegration(), "transactions and subscriptions require native TCP/TLS");

        try (FerricStoreClient client = connectRaw()) {
            String suffix = suffix();
            String key = "{java-sdk:session:" + suffix + "}:transaction";
            try (FerricStoreTransaction transaction = client.transaction(List.of(key))) {
                transaction.command("SET", key, bytes("value"));
                transaction.command("GET", key);
                List<Object> results = transaction.execute();
                assertEquals(2, results.size());
                assertTrue(ok(results.get(0)));
                assertEquals("value", text(results.get(1)));
            }

            String channel = "java-sdk:session:" + suffix + ":events";
            try (FerricStorePubSub pubsub = client.pubsubSession()) {
                pubsub.subscribe(channel);
                assertEquals(1, client.publish(channel, "payload"));
                PubSubMessage message = pubsub.getMessage(Duration.ofSeconds(5));
                assertNotNull(message);
                assertEquals("message", message.kind());
                assertEquals(channel, message.channel());
                assertEquals("payload", text(message.message()));
            }
        }
    }

    @Test
    void everyCataloguedFlowCommandIsRecognizedByTheSelectedTransport() {
        assumeIntegration();

        try (FerricStoreClient client = connectRaw()) {
            List<String> rejected = new ArrayList<>();
            for (FlowCommand command : FlowCommand.values()) {
                try {
                    client.command(command);
                } catch (FerricStoreException error) {
                    String message = String.valueOf(error.getMessage()).toLowerCase(Locale.ROOT);
                    if (message.contains("unknown command")
                            || message.contains("unsupported over stateless http")
                            || message.contains("unsupported_command")) {
                        rejected.add(command.wireName() + ": " + error.getMessage());
                    }
                }
            }
            assertTrue(rejected.isEmpty(), () -> "transport rejected Flow commands: " + rejected);
        }
    }

    private static void assertStringCommands(FerricStoreClient client, String prefix) {
        String key = prefix + "string";
        assertEquals(
                true,
                client.kv().set(key, "abc", SetOptions.builder().pxMilliseconds(60_000L).build()));
        assertEquals("abc", client.kv().get(key, String.class));
        assertEquals(1, client.kv().exists(key));
        assertEquals("abc", text(client.kv().mget(List.of(key, prefix + "missing")).get(0)));
        assertTrue(ok(client.command("MSET", prefix + "string2", "2", prefix + "string3", "3")));
        assertEquals(1, number(client.command("MSETNX", prefix + "nx1", "1", prefix + "nx2", "2")));
        assertEquals(1, client.kv().incr(prefix + "counter"));
        assertEquals(5, client.kv().incrBy(prefix + "counter", 4));
        assertEquals(4, client.kv().decr(prefix + "counter"));
        assertEquals(2, client.kv().decrBy(prefix + "counter", 2));
        assertTrue(Resp.decimal(client.command("INCRBYFLOAT", prefix + "float", "1.5")) >= 1.5);
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
        assertEquals("value", text(list(client.command("HMGET", key, "field", "none")).get(0)));
        assertNotNull(client.hash().hgetall(key));
        assertTrue(client.hash().hexists(key, "field"));
        assertFalse(client.hash().hkeys(key).isEmpty());
        assertFalse(list(client.command("HVALS", key)).isEmpty());
        assertTrue(client.hash().hlen(key) >= 2);
        assertEquals(3, client.hash().hincrBy(key, "count", 2));
        assertTrue(Resp.decimal(client.command("HINCRBYFLOAT", key, "float", "1.25")) >= 1.25);
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
                                .get(0)));
        assertTrue(number(client.command("HSETEX", key, 60, "temp", "1")) >= 0);
        assertEquals("1", text(list(client.command("HGETDEL", key, "FIELDS", 1, "temp")).get(0)));
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
        if (!isHttpIntegration()) {
            assertNotNull(client.lists().blpop(List.of(listKey), 1));
            assertTrue(client.lists().rpush(listKey, "block") >= 1);
            assertNotNull(client.lists().brpop(List.of(listKey), 1));
            assertTrue(client.lists().rpush(listKey, "move") >= 1);
            assertNotNull(client.lists().blmove(listKey, listDst, "LEFT", "RIGHT", 1));
            assertTrue(client.lists().rpush(listKey, "mpop") >= 1);
            assertNotNull(client.lists().blmpop(1, List.of(listKey), "LEFT", 1));
        }
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
        if (!isHttpIntegration()) {
            assertNotNull(client.stream().xread(Map.of(stream, "0-0"), 1, null));
        }
        assertNotNull(client.command("XINFO", "STREAM", stream));
        String group = "group-" + suffix;
        assertTrue(ok(client.command("XGROUP", "CREATE", stream, group, "0")));
        if (!isHttpIntegration()) {
            assertNotNull(
                    client.stream()
                            .xreadgroup(group, "consumer", Map.of(stream, ">"), 1, null, false));
        }
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

    private static void assertJsonDocuments(FerricStoreClient client, String prefix) {
        String first = prefix + "json-one";
        String second = prefix + "json-two";
        assertTrue(client.json().set(first, "$", Map.of("name", "Ada", "active", true)));
        assertTrue(client.json().set(second, "$", Map.of("name", "Grace", "active", false)));
        assertEquals("Ada", client.json().get(first, Map.class).get("name"));
        assertEquals(2, client.json().mget(List.of(first, second), "$").size());
        assertEquals(1, client.json().del(second, "$"));
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

    private static void assertFlowInsights(
            FerricStoreClient client, String type, String partition, String suffix, long now) {
        assertNotNull(
                client.installPolicy(
                        type,
                        FlowPolicyOptions.builder()
                                .indexedAttribute("tenant")
                                .indexedStateMeta("version")
                                .retry(new RetryPolicy(2, "fixed", 10L, 100L, 0, "failed"))
                                .statePolicy("claim-attributes", FlowStatePolicy.fifo())
                                .build()));
        assertNotNull(client.policyGet(type, "claim-attributes"));

        String id = "java-sdk:admin:attributes:" + suffix;
        assertNotNull(
                client.create(
                        CreateOptions.builder(id, type)
                                .state("attributes")
                                .partitionKey(partition)
                                .attribute("tenant", "acme")
                                .stateMeta("version", "1")
                                .nowMs(now)
                                .runAtMs(now)
                                .idempotent(true)
                                .build()));

        assertEventuallyContains(() -> client.list(type, "attributes", partition, 20), id);
        assertNotNull(
                client.flowInsights()
                        .stats(
                                type,
                                FlowInsights.StatsOptions.builder()
                                        .state("attributes")
                                        .partitionKey(partition)
                                        .attribute("tenant", "acme")
                                        .consistentProjection(true)
                                        .build()));
        assertNotNull(
                client.flowInsights()
                        .attributes(
                                type,
                                FlowInsights.ReadOptions.builder()
                                        .state("attributes")
                                        .partitionKey(partition)
                                        .consistentProjection(true)
                                        .build()));
        assertNotNull(
                client.flowInsights()
                        .attributeValues(
                                type,
                                "tenant",
                                FlowInsights.ReadOptions.builder()
                                        .state("attributes")
                                        .partitionKey(partition)
                                        .consistentProjection(true)
                                        .build()));

        String claimId = "java-sdk:admin:claim-attributes:" + suffix;
        client.create(
                CreateOptions.builder(claimId, type)
                        .state("claim-attributes")
                        .partitionKey(partition)
                        .attribute("tenant", "acme")
                        .nowMs(now)
                        .runAtMs(now)
                        .build());
        List<ClaimedItem> claimed =
                client.claimJobs(
                        ClaimDueOptions.builder(type, "java-sdk-attribute-worker")
                                .state("claim-attributes")
                                .partitionKey(partition)
                                .includeAttributes(true)
                                .nowMs(now)
                                .build());
        assertEquals(1, claimed.size());
        assertEquals("acme", text(claimed.get(0).attributes().get("tenant")));
    }

    private static void assertFlowSteps(
            FerricStoreClient client, String type, String partition, String suffix, long now) {
        String id = "java-sdk:admin:step:" + suffix;
        FlowRecord started =
                client.startAndClaim(
                        StartAndClaimOptions.builder(id, type, "step-a", "java-sdk-step-worker")
                                .partitionKey(partition)
                                .payload(Map.of("step", "a"))
                                .nowMs(now)
                                .build());
        assertNotNull(started);

        ClaimedItem continued =
                (ClaimedItem)
                        client.flowSteps()
                                .continueStep(
                                        FlowSteps.ContinueOptions.builder(
                                                        started.id(),
                                                        started.leaseToken(),
                                                        started.fencingToken(),
                                                        "step-a",
                                                        "step-b")
                                                .partitionKey(partition)
                                                .worker("java-sdk-step-worker")
                                                .payload(Map.of("step", "b"))
                                                .returnJob(true)
                                                .nowMs(now + 1)
                                                .build());
        assertEquals(id, continued.id());
        assertNotNull(
                client.complete(
                        CompleteOptions.builder(
                                        continued.id(),
                                        continued.leaseToken(),
                                        continued.fencingToken())
                                .partitionKey(partition)
                                .result(Map.of("done", true))
                                .nowMs(now + 2)
                                .build()));

        assertNotNull(
                client.flowSteps()
                        .runMany(
                                FlowSteps.RunManyOptions.builder(
                                                type,
                                                List.of(
                                                        new FlowSteps.RunItem(
                                                                "java-sdk:admin:run-many:" + suffix,
                                                                partition)))
                                        .states(List.of("queued", "done"))
                                        .worker("java-sdk-run-many-worker")
                                        .nowMs(now)
                                        .result(Map.of("done", true))
                                        .build()));
    }

    private static void assertFlowSchedules(
            FerricStoreClient client, String type, String partition, String suffix, long now) {
        FlowSchedules schedules = client.flowSchedules();
        String scheduleId = "java-sdk:admin:schedule:" + suffix;
        Map<String, Object> target =
                Map.of(
                        "id",
                        "java-sdk:admin:scheduled:" + suffix,
                        "type",
                        type,
                        "state",
                        "scheduled",
                        "partition_key",
                        partition,
                        "payload",
                        Map.of("scheduled", true));
        assertNotNull(
                schedules.create(
                        scheduleId,
                        FlowSchedules.CreateOptions.builder(target)
                                .kind("one_shot")
                                .atMs(now + 60_000)
                                .overwrite(true)
                                .nowMs(now)
                                .build()));
        assertNotNull(schedules.get(scheduleId));
        assertNotNull(schedules.pause(scheduleId, now + 1));
        assertNotNull(schedules.resume(scheduleId, now + 2));
        assertNotNull(schedules.list(FlowSchedules.ListOptions.builder().count(10).build()));

        String deleteId = scheduleId + ":delete";
        assertNotNull(
                schedules.create(
                        deleteId,
                        FlowSchedules.CreateOptions.builder(
                                        Map.of(
                                                "id",
                                                "java-sdk:admin:scheduled-delete:" + suffix,
                                                "type",
                                                type,
                                                "state",
                                                "scheduled",
                                                "partition_key",
                                                partition))
                                .kind("one_shot")
                                .atMs(now + 120_000)
                                .overwrite(true)
                                .nowMs(now)
                                .build()));
        schedules.delete(deleteId, now + 3);
        assertNotNull(schedules.fire(scheduleId, null, now + 3));
        assertNotNull(
                schedules.fireDue(
                        FlowSchedules.FireDueOptions.builder()
                                .nowMs(now + 4)
                                .worker("java-sdk-scheduler")
                                .limit(10)
                                .build()));
    }

    private static void assertFlowGovernance(
            FerricStoreClient client, String type, String suffix, long now) {
        ClaimedFlow flow =
                createAndClaim(client, type, suffix, "governance", "governance", now, 30_000);
        String id = flow.id();
        String partition = flow.partitionKey();
        ClaimedItem job = flow.job();

        EffectReserveOptions reserve =
                EffectReserveOptions.builder(job.leaseToken(), job.fencingToken())
                        .partitionKey(partition)
                        .operationDigest("sha256:email")
                        .idempotencyKey("java-sdk:admin:effect:" + suffix)
                        .nowMs(now + 10)
                        .build();
        assertNotNull(client.effectReserve(id, "email", "email.send", reserve));
        assertNotNull(
                client.effectConfirm(
                        id,
                        "email",
                        EffectStatusOptions.builder()
                                .partitionKey(partition)
                                .lease(job.leaseToken(), job.fencingToken())
                                .externalId("mail-1")
                                .latencyMs(12)
                                .nowMs(now + 11)
                                .build()));
        assertNotNull(client.effectGet(id, "email", partition));

        assertNotNull(
                client.effectReserve(
                        id,
                        "push",
                        "push.send",
                        EffectReserveOptions.builder(job.leaseToken(), job.fencingToken())
                                .partitionKey(partition)
                                .operationDigest("sha256:push")
                                .idempotencyKey("java-sdk:admin:push:" + suffix)
                                .nowMs(now + 12)
                                .build()));
        assertNotNull(
                client.effectCompensate(
                        id,
                        "push",
                        EffectStatusOptions.builder()
                                .partitionKey(partition)
                                .lease(job.leaseToken(), job.fencingToken())
                                .reason("rollback")
                                .nowMs(now + 13)
                                .build()));

        assertNotNull(
                client.effectReserve(
                        id,
                        "sms",
                        "sms.send",
                        EffectReserveOptions.builder(job.leaseToken(), job.fencingToken())
                                .partitionKey(partition)
                                .operationDigest("sha256:sms")
                                .idempotencyKey("java-sdk:admin:sms:" + suffix)
                                .nowMs(now + 14)
                                .build()));
        assertNotNull(
                client.effectFail(
                        id,
                        "sms",
                        EffectStatusOptions.builder()
                                .partitionKey(partition)
                                .lease(job.leaseToken(), job.fencingToken())
                                .error("provider-error")
                                .latencyMs(20)
                                .nowMs(now + 15)
                                .build()));

        FlowGovernance governance = client.flowGovernance();
        assertNotNull(
                governance.ledger(
                        id,
                        FlowGovernance.LedgerOptions.builder()
                                .partitionKey(partition)
                                .limit(20)
                                .build()));
        String approvalScope = "java-sdk:approval:" + suffix;
        String approvalId = "java-sdk:admin:approval:" + suffix;
        assertNotNull(
                governance.approvalRequest(
                        approvalId,
                        FlowGovernance.ApprovalRequestOptions.builder(id, approvalScope)
                                .reason("manual check")
                                .requestedBy("integration")
                                .assignees(List.of("ops"))
                                .nowMs(now + 16)
                                .build()));
        assertNotNull(governance.approvalGet(approvalId));
        assertNotNull(
                governance.approvalList(
                        FlowGovernance.ApprovalListOptions.builder()
                                .scope(approvalScope)
                                .limit(10)
                                .build()));
        assertNotNull(governance.approvalApprove(approvalId, "ops", "ok", now + 17));

        String rejectedId = approvalId + ":rejected";
        assertNotNull(
                governance.approvalRequest(
                        rejectedId,
                        FlowGovernance.ApprovalRequestOptions.builder(id, approvalScope)
                                .reason("manual reject")
                                .requestedBy("integration")
                                .nowMs(now + 18)
                                .build()));
        assertNotNull(governance.approvalReject(rejectedId, "ops", "no", now + 19));

        String circuitScope = "java-sdk:circuit:" + suffix;
        assertNotNull(
                governance.circuitOpen(
                        circuitScope,
                        FlowGovernance.CircuitOpenOptions.builder()
                                .openMs(1_000)
                                .nowMs(now + 20)
                                .build()));
        assertNotNull(governance.circuitGet(circuitScope));
        assertNotNull(governance.circuitClose(circuitScope, now + 21));

        String budgetScope = "java-sdk:budget:" + suffix;
        String commitReservation = "java-sdk:reservation:" + suffix + ":commit";
        assertNotNull(
                governance.budgetReserve(
                        budgetScope,
                        5,
                        FlowGovernance.BudgetReserveOptions.builder()
                                .limit(100)
                                .windowMs(60_000)
                                .reservationId(commitReservation)
                                .nowMs(now + 22)
                                .build()));
        assertNotNull(
                governance.budgetCommit(
                        budgetScope, commitReservation, 4, Map.of("tokens", 4), now + 23));
        String releaseReservation = "java-sdk:reservation:" + suffix + ":release";
        assertNotNull(
                governance.budgetReserve(
                        budgetScope,
                        3,
                        FlowGovernance.BudgetReserveOptions.builder()
                                .limit(100)
                                .windowMs(60_000)
                                .reservationId(releaseReservation)
                                .nowMs(now + 24)
                                .build()));
        assertNotNull(governance.budgetRelease(budgetScope, releaseReservation, now + 25));
        assertNotNull(governance.budgetGet(budgetScope));
        assertNotNull(
                governance.budgetList(
                        FlowGovernance.FilterOptions.builder()
                                .scope(budgetScope)
                                .limit(10)
                                .build()));

        String limitScope = "java-sdk:limit:" + suffix;
        assertNotNull(governance.limitLease(limitScope, 0, 5, 30_000, 10L, now + 26));
        Map<String, Object> spent = governance.limitSpend(limitScope, 0, 2, now + 27);
        List<String> reservationIds =
                list(field(spent, "reservation_ids")).stream()
                        .map(FerricStoreIntegrationTest::text)
                        .toList();
        assertEquals(2, reservationIds.size());
        assertNotNull(governance.limitRelease(limitScope, 0, reservationIds, now + 28));
        assertNotNull(governance.limitGet(limitScope, now + 29));
        assertNotNull(
                governance.limitList(
                        FlowGovernance.FilterOptions.builder()
                                .scope(limitScope)
                                .limit(10)
                                .nowMs(now + 30)
                                .build()));
        assertNotNull(
                governance.overview(
                        FlowGovernance.ApprovalListOptions.builder().limit(10).build()));
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
                                .mutationFields(
                                        FlowMutationFields.builder()
                                                .attributeMerge("stage", "ready")
                                                .stateMeta("attempt", 1)
                                                .build())
                                .build()));
        FlowRecord transitioned = client.get(transition.id(), transition.partitionKey());
        assertEquals("ready", text(transitioned.attributes().get("stage")));
        assertNotNull(transitioned.stateMeta().get("ready"));
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
        assertEventuallyContains(
                () -> client.failures(type, failed.partitionKey(), 20), failed.id());

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
        assertEventuallyContains(
                () -> client.terminals(type, null, cancelled.partitionKey(), 50), cancelled.id());
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
                                        manyJobs.get(0).state(),
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
        ClaimedItem reclaimedJob = reclaimed.get(0);
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
        assertEventuallyContains(
                () -> client.stuck(type, stuckPartition, 10, 1L, 120_000L), stuckId);
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
        assertEventuallyNotEmpty(() -> client.byParent(parentId, parentPartition, 20));
        assertEventuallyNotEmpty(() -> client.byRoot("root:" + suffix, parentPartition, 20));
        assertEventuallyNotEmpty(() -> client.byCorrelation("corr:" + suffix, parentPartition, 20));

        ClaimedFlow rewind = createAndClaim(client, type, suffix, "rewind", "queued", now, 30_000);
        String createdEventId =
                eventId(client.history(rewind.id(), rewind.partitionKey(), 10).get(0));
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

    private static void assertEventuallyContains(
            Supplier<List<FlowRecord>> records, String expectedId) {
        assertTrue(
                awaitQuery(records).stream().anyMatch(record -> record.id().equals(expectedId)),
                () -> "query did not expose expected Flow " + expectedId);
    }

    private static void assertEventuallyNotEmpty(Supplier<List<FlowRecord>> records) {
        assertFalse(awaitQuery(records).isEmpty(), "query did not expose any matching Flows");
    }

    private static List<FlowRecord> awaitQuery(Supplier<List<FlowRecord>> records) {
        long deadline = System.nanoTime() + 5_000_000_000L;
        List<FlowRecord> current;
        do {
            current = records.get();
            if (!current.isEmpty()) {
                return current;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(
                        "interrupted while waiting for Flow query projection", error);
            }
        } while (System.nanoTime() < deadline);
        return current;
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
        return jobs.get(0);
    }

    private static FencedItem fenced(ClaimedItem job) {
        return new FencedItem(job.id(), job.fencingToken(), job.leaseToken(), job.partitionKey());
    }

    private static String eventId(Object event) {
        if (event instanceof List<?> list && !list.isEmpty()) {
            return text(list.get(0));
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void deletePrefixedKeys(FerricStoreClient client, String prefix) {
        List<Object> keys = client.kv().keys(prefix + "*");
        for (int start = 0; start < keys.size(); start += 64) {
            deleteKeyRange(client, keys, start, Math.min(start + 64, keys.size()));
        }
    }

    private static void deleteKeyRange(
            FerricStoreClient client, List<Object> keys, int start, int end) {
        List<Object> command = new ArrayList<>();
        command.add("DEL");
        command.addAll(keys.subList(start, end));
        client.command(command);
    }

    private static FerricStoreClient connectJson() {
        return connect(new JsonCodec());
    }

    private static FerricStoreClient connectRaw() {
        return connect(new RawCodec());
    }

    private static FerricStoreClient connect(Codec codec) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
        String caFile = System.getenv("FERRICSTORE_CA_FILE");
        if (!(url.startsWith("http://") || url.startsWith("https://"))) {
            NativeTransportOptions.Builder options = NativeTransportOptions.builder();
            if (caFile != null && !caFile.isBlank()) {
                options.sslContext(trustContext(Path.of(caFile)));
            }
            return FerricStoreClient.connect(url, codec, options.build());
        }
        HttpTransportOptions.Builder options =
                HttpTransportOptions.builder()
                        .username(requiredEnvironment("FERRICSTORE_USERNAME"))
                        .password(requiredEnvironment("FERRICSTORE_PASSWORD"));
        if (url.startsWith("http://")) {
            options.allowInsecureBasicAuthentication(true);
        }
        if (caFile != null && !caFile.isBlank()) {
            options.sslContext(trustContext(Path.of(caFile)));
        }
        return FerricStoreClient.connect(url, codec, options.build());
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for HTTP integration tests");
        }
        return value;
    }

    private static SSLContext trustContext(Path certificateFile) {
        try (java.io.InputStream input = Files.newInputStream(certificateFile)) {
            Certificate certificate =
                    CertificateFactory.getInstance("X.509").generateCertificate(input);
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ferricstore-http", certificate);
            TrustManagerFactory trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagers.getTrustManagers(), null);
            return context;
        } catch (java.io.IOException | java.security.GeneralSecurityException error) {
            throw new IllegalStateException("failed to load FerricStore integration CA", error);
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static void assumeIntegration() {
        assumeTrue(
                "1".equals(System.getenv("FERRICSTORE_INTEGRATION")),
                "set FERRICSTORE_INTEGRATION=1 to run local FerricStore integration tests");
    }

    private static boolean isHttpIntegration() {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
        return url.startsWith("http://") || url.startsWith("https://");
    }

    private record ClaimedFlow(String id, String partitionKey, ClaimedItem job) {}
}
