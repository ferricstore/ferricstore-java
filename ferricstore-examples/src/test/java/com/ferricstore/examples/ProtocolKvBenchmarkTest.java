package com.ferricstore.examples;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ProtocolKvBenchmarkTest {
    @Test
    void throughputPresetMatchesThePythonWorkloadShape() {
        ProtocolKvBenchmark.Config config =
                ProtocolKvBenchmark.Config.parse(
                        new String[] {
                            "--preset", "get-throughput", "--url", "ferric://localhost:6388"
                        });

        assertEquals("get", config.command());
        assertEquals(1_000, config.batchSize());
        assertEquals(64, config.inflightBatches());
        assertEquals(30.0, config.durationSeconds());
        assertEquals(100_000, config.keyCount());
    }

    @Test
    void explicitOptionsOverridePresetDefaults() {
        ProtocolKvBenchmark.Config config =
                ProtocolKvBenchmark.Config.parse(
                        new String[] {
                            "--preset",
                            "set-throughput",
                            "--batch-size",
                            "25",
                            "--inflight-batches",
                            "7",
                            "--duration-seconds",
                            "1.5",
                            "--http-version",
                            "2"
                        });

        assertEquals(25, config.batchSize());
        assertEquals(7, config.inflightBatches());
        assertEquals(1.5, config.durationSeconds());
        assertEquals("2", config.httpVersion());
    }

    @Test
    void buildsBinarySafeGetSetAndMixedCommands() {
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        assertEquals(
                List.of("GET", "bench:2"), ProtocolKvBenchmark.command("get", "bench:2", value, 0));
        List<Object> set = ProtocolKvBenchmark.command("set", "bench:2", value, 0);
        assertEquals(List.of("SET", "bench:2"), set.subList(0, 2));
        assertArrayEquals(value, (byte[]) set.get(2));
        assertEquals("GET", ProtocolKvBenchmark.command("mixed", "bench:2", value, 2).get(0));
        assertEquals("SET", ProtocolKvBenchmark.command("mixed", "bench:3", value, 3).get(0));
    }

    @Test
    void usesNearestRankPercentiles() {
        assertEquals(0.0, ProtocolKvBenchmark.percentile(List.of(), 99));
        assertEquals(4.0, ProtocolKvBenchmark.percentile(List.of(4.0, 1.0, 3.0, 2.0), 95));
        assertEquals(2.0, ProtocolKvBenchmark.percentile(List.of(4.0, 1.0, 3.0, 2.0), 50));
    }

    @Test
    void rejectsUnsafeOrImpossibleShapes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolKvBenchmark.Config.parse(new String[] {"--batch-size", "0"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolKvBenchmark.Config.parse(new String[] {"--command", "delete"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolKvBenchmark.Config.parse(new String[] {"--http-version", "3"}));
    }
}
