package com.ferricstore;

import static com.ferricstore.IntegrationTestEnvironment.assumeIntegration;
import static com.ferricstore.IntegrationTestEnvironment.connectRaw;
import static com.ferricstore.IntegrationTestEnvironment.isHttpIntegration;
import static com.ferricstore.IntegrationTestEnvironment.suffix;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

final class FerricStoreConcurrencyIntegrationTest {
    private static final int CONCURRENCY = 32;
    private static final int HTTP_BLOCKERS = 16;
    private static final int ASYNC_OPERATIONS = 256;
    private static final int OPERATIONS = 1_024;
    private static final Pattern BLOCKED_CLIENTS =
            Pattern.compile("(?m)^blocked_clients:(\\d+)\\s*$");

    @Test
    void asyncSharedClientNeedsNoWaitingThreadPerRequest() throws Exception {
        assumeIntegration();

        String testId = suffix();
        String counterKey = "java-sdk:async:{" + testId + "}:counter";
        try (FerricStoreClient client = connectRaw()) {
            client.command("DEL", counterKey);
            List<CompletableFuture<Object>> echoes = new ArrayList<>(ASYNC_OPERATIONS);
            for (int operation = 0; operation < ASYNC_OPERATIONS; operation++) {
                echoes.add(client.commandAsync("ECHO", bytes("async-" + testId + '-' + operation)));
            }
            CompletableFuture.allOf(echoes.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
            for (int operation = 0; operation < ASYNC_OPERATIONS; operation++) {
                assertBytesEqual(
                        bytes("async-" + testId + '-' + operation), echoes.get(operation).join());
            }

            List<CompletableFuture<Object>> increments = new ArrayList<>(ASYNC_OPERATIONS);
            for (int operation = 0; operation < ASYNC_OPERATIONS; operation++) {
                increments.add(client.commandAsync("INCR", counterKey));
            }
            CompletableFuture.allOf(increments.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
            Set<Long> values = new HashSet<>();
            increments.forEach(result -> values.add(Resp.number(result.join())));
            assertEquals(ASYNC_OPERATIONS, values.size());
            assertEquals(ASYNC_OPERATIONS, Resp.number(client.command("GET", counterKey)));
            client.command("DEL", counterKey);
        }
    }

    @Test
    void oneSharedClientCorrelatesConcurrentResponsesAndPreservesAtomicUpdates() throws Exception {
        assumeIntegration();

        String testId = suffix();
        String counterKey = "java-sdk:concurrency:{" + testId + "}:counter";
        ExecutorService callers = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch ready = new CountDownLatch(CONCURRENCY);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();

        try (FerricStoreClient client = connectRaw()) {
            client.command("DEL", counterKey);
            List<Future<Long>> results = new ArrayList<>(OPERATIONS);
            for (int operation = 0; operation < OPERATIONS; operation++) {
                int id = operation;
                results.add(
                        callers.submit(
                                () -> {
                                    ready.countDown();
                                    if (!start.await(10, TimeUnit.SECONDS)) {
                                        throw new AssertionError("concurrent start gate timed out");
                                    }
                                    int current = active.incrementAndGet();
                                    maximumActive.accumulateAndGet(current, Math::max);
                                    try {
                                        byte[] expected = bytes("echo-" + testId + "-" + id);
                                        assertBytesEqual(
                                                expected, client.command("ECHO", expected));
                                        return Resp.number(client.command("INCR", counterKey));
                                    } finally {
                                        active.decrementAndGet();
                                    }
                                }));
            }

            assertTrue(
                    ready.await(10, TimeUnit.SECONDS), "concurrent callers did not become ready");
            start.countDown();

            Set<Long> increments = new HashSet<>();
            for (Future<Long> result : results) {
                increments.add(result.get(30, TimeUnit.SECONDS));
            }

            assertTrue(maximumActive.get() > 1, "requests never overlapped at the shared client");
            assertEquals(OPERATIONS, increments.size());
            assertEquals(1L, increments.stream().mapToLong(Long::longValue).min().orElseThrow());
            assertEquals(
                    OPERATIONS, increments.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertEquals(OPERATIONS, Resp.number(client.command("GET", counterKey)));
            client.command("DEL", counterKey);
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void httpSharedClientRunsRequestsConcurrentlyAtTheServer() throws Exception {
        assumeIntegration();
        assumeTrue(isHttpIntegration(), "server-side HTTP concurrency test");

        String testId = suffix();
        List<String> keys = new ArrayList<>(HTTP_BLOCKERS);
        for (int index = 0; index < HTTP_BLOCKERS; index++) {
            keys.add("java-sdk:concurrency:http:block:" + testId + ':' + index);
        }
        ExecutorService callers = Executors.newFixedThreadPool(HTTP_BLOCKERS);
        CountDownLatch ready = new CountDownLatch(HTTP_BLOCKERS);
        CountDownLatch start = new CountDownLatch(1);

        try (FerricStoreClient shared = connectRaw();
                FerricStoreClient observer = connectRaw()) {
            observer.command(commandWithKeys("DEL", keys));
            long initialBlocked = blockedClientCount(observer);
            List<Future<Object>> blocked = new ArrayList<>(HTTP_BLOCKERS);
            for (String key : keys) {
                blocked.add(
                        callers.submit(
                                () -> {
                                    ready.countDown();
                                    if (!start.await(10, TimeUnit.SECONDS)) {
                                        throw new AssertionError("concurrent start gate timed out");
                                    }
                                    return shared.command("BLPOP", key, 10);
                                }));
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS), "HTTP callers did not become ready");
            start.countDown();
            awaitBlockedClientCount(
                    observer, initialBlocked + HTTP_BLOCKERS, Duration.ofSeconds(5));

            for (int index = 0; index < HTTP_BLOCKERS; index++) {
                assertEquals(
                        1L,
                        Resp.number(
                                observer.command(
                                        "RPUSH", keys.get(index), bytes("release-" + index))));
            }
            for (int index = 0; index < HTTP_BLOCKERS; index++) {
                List<Object> response = Resp.list(blocked.get(index).get(5, TimeUnit.SECONDS));
                assertBytesEqual(bytes(keys.get(index)), response.get(0));
                assertBytesEqual(bytes("release-" + index), response.get(1));
            }
            observer.command(commandWithKeys("DEL", keys));
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void nativeBlockedLaneDoesNotStallAnIndependentLaneOnTheSameConnection() throws Exception {
        assumeIntegration();
        assumeFalse(isHttpIntegration(), "native lane behavior does not apply to HTTP");

        String testId = suffix();
        String blockedKey = "java-sdk:concurrency:block:" + testId;
        String independentKey = keyOnDifferentLane(blockedKey, testId);
        ExecutorService callers = Executors.newFixedThreadPool(2);

        try (FerricStoreClient shared = connectRaw();
                FerricStoreClient observer = connectRaw()) {
            observer.command("DEL", blockedKey, independentKey);
            long initialBlocked = blockedClientCount(observer);
            Future<Object> blocked =
                    callers.submit(() -> shared.lists().blpop(List.of(blockedKey), 10));

            awaitBlockedClientCount(observer, initialBlocked + 1, Duration.ofSeconds(5));
            Future<Object> independent =
                    callers.submit(() -> shared.command("SET", independentKey, bytes("ready")));

            assertTrue(CommandArgs.ok(independent.get(2, TimeUnit.SECONDS)));
            assertFalse(blocked.isDone(), "blocking lane completed before it was released");
            assertEquals(1L, Resp.number(observer.command("LPUSH", blockedKey, bytes("release"))));

            List<Object> response = Resp.list(blocked.get(5, TimeUnit.SECONDS));
            assertBytesEqual(bytes(blockedKey), response.get(0));
            assertBytesEqual(bytes("release"), response.get(1));
            observer.command("DEL", blockedKey, independentKey);
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static String keyOnDifferentLane(String blockedKey, String testId) {
        int blockedLane = lane(blockedKey);
        for (int candidate = 0; candidate < 1_000; candidate++) {
            String key = "java-sdk:concurrency:independent:" + testId + ':' + candidate;
            if (lane(key) != blockedLane) {
                return key;
            }
        }
        throw new AssertionError("could not construct a key on a different native lane");
    }

    private static int lane(String key) {
        return 1 + Math.floorMod(key.hashCode(), 32);
    }

    private static List<Object> commandWithKeys(String command, List<String> keys) {
        List<Object> args = new ArrayList<>(keys.size() + 1);
        args.add(command);
        args.addAll(keys);
        return args;
    }

    private static void awaitBlockedClientCount(
            FerricStoreClient observer, long expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (blockedClientCount(observer) >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(
                blockedClientCount(observer) >= expected,
                "native blocking request did not reach FerricStore");
    }

    private static long blockedClientCount(FerricStoreClient client) {
        Matcher matcher = BLOCKED_CLIENTS.matcher(client.serverInfo("clients"));
        assertTrue(matcher.find(), "INFO clients did not contain blocked_clients");
        return Long.parseLong(matcher.group(1));
    }

    private static void assertBytesEqual(byte[] expected, Object actual) {
        assertTrue(actual instanceof byte[], "expected a binary response");
        assertArrayEquals(expected, (byte[]) actual);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
