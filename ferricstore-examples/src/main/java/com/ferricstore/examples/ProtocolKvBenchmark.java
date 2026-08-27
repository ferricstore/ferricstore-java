package com.ferricstore.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ferricstore.FerricStoreClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/** Real-server asynchronous SET/GET benchmark comparable to the Python SDK workload shapes. */
public final class ProtocolKvBenchmark {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DEFAULT_URL = "ferric://127.0.0.1:6388";
    private static final String DEFAULT_PREFIX = "java-kv-benchmark";
    private static final int WARMUP_BATCH_SIZE = 100;
    private static final int MAX_WARMUP_BATCHES_IN_FLIGHT = 8;

    private ProtocolKvBenchmark() {}

    @SuppressWarnings("PMD.SystemPrintln") // JSON is the benchmark's machine-readable output.
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Config config = Config.parse(args);
        Map<String, Object> result = run(config);
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    static Map<String, Object> run(Config config) throws InterruptedException {
        byte[] value = new byte[config.valueBytes()];
        Arrays.fill(value, (byte) 'x');
        List<String> keys = keys(config.keyPrefix(), config.keyCount());

        try (FerricStoreClient client = connect(config)) {
            verifyRoundTrip(client, config.keyPrefix(), value);
            int warmedKeys = config.warmup() ? warm(client, config, keys, value) : 0;
            return measure(client, config, keys, value, warmedKeys);
        }
    }

    private static FerricStoreClient connect(Config config) {
        return BenchmarkClients.connect(
                config.url(), config.httpVersion(), config.inflightBatches(), config.batchSize());
    }

    private static void verifyRoundTrip(
            FerricStoreClient client, String keyPrefix, byte[] expectedValue) {
        String key = keyPrefix + ":correctness";
        client.command("SET", key, expectedValue);
        Object actual = client.command("GET", key);
        if (!(actual instanceof byte[] bytes) || !Arrays.equals(expectedValue, bytes)) {
            throw new IllegalStateException("benchmark correctness probe returned a wrong value");
        }
    }

    @SuppressWarnings(
            "PMD.AvoidInstantiatingObjectsInLoops") // Each future needs its own result metadata.
    private static int warm(
            FerricStoreClient client, Config config, List<String> keys, byte[] value) {
        if ("set".equals(config.command())) {
            return 0;
        }
        List<WarmupBatch> pending = new ArrayList<>();
        for (int start = 0; start < keys.size(); start += WARMUP_BATCH_SIZE) {
            int end = Math.min(start + WARMUP_BATCH_SIZE, keys.size());
            List<List<Object>> commands = new ArrayList<>(end - start);
            for (int index = start; index < end; index++) {
                commands.add(List.of("SET", keys.get(index), value));
            }
            pending.add(new WarmupBatch(client.pipelineAsync(commands), commands.size()));
            if (pending.size()
                    == Math.min(config.inflightBatches(), MAX_WARMUP_BATCHES_IN_FLIGHT)) {
                awaitWarmup(pending);
                pending.clear();
            }
        }
        awaitWarmup(pending);
        return keys.size();
    }

    private static void awaitWarmup(List<WarmupBatch> pending) {
        for (WarmupBatch batch : pending) {
            if (batch.future().join().size() != batch.expected()) {
                throw new IllegalStateException("benchmark warmup returned an incomplete batch");
            }
        }
    }

    @SuppressWarnings(
            "PMD.NcssCount") // Keeping the timed loop together avoids benchmark-distorting helpers.
    private static Map<String, Object> measure(
            FerricStoreClient client,
            Config config,
            List<String> keys,
            byte[] value,
            int warmedKeys)
            throws InterruptedException {
        LinkedBlockingQueue<CompletedBatch> completions = new LinkedBlockingQueue<>();
        List<Double> batchLatencies = new ArrayList<>();
        long durationNanos = secondsToNanos(config.durationSeconds());
        long cpuStarted = processCpuNanos();
        long started = System.nanoTime();
        long deadline = started + durationNanos;
        long sequence = 0;
        long issued = 0;
        long completed = 0;
        long errors = 0;
        int pending = 0;
        String firstError = null;

        while (System.nanoTime() < deadline || pending > 0) {
            while (pending < config.inflightBatches() && System.nanoTime() < deadline) {
                List<List<Object>> commands =
                        commandBatch(config, keys, value, sequence, config.batchSize());
                long batchStarted = System.nanoTime();
                CompletableFuture<List<Object>> future = client.pipelineAsync(commands);
                int batchSize = commands.size();
                future.whenComplete(
                        (responses, failure) ->
                                completions.add(
                                        new CompletedBatch(
                                                batchStarted, batchSize, responses, failure)));
                sequence += batchSize;
                issued += batchSize;
                pending++;
            }
            if (pending == 0) {
                break;
            }
            CompletedBatch batch = completions.take();
            pending--;
            batchLatencies.add((System.nanoTime() - batch.startedNanos()) / 1_000_000.0);
            if (batch.failure() != null) {
                errors += batch.size();
                if (firstError == null) {
                    firstError = rootMessage(batch.failure());
                }
            } else if (batch.responses() == null || batch.responses().size() != batch.size()) {
                errors += batch.size();
                if (firstError == null) {
                    firstError = "incomplete pipeline response";
                }
            } else {
                completed += batch.size();
            }
        }

        long finished = System.nanoTime();
        double seconds = Math.max((finished - started) / 1_000_000_000.0, 1.0e-9);
        long cpuFinished = processCpuNanos();
        double cpuSeconds = Math.max(cpuFinished - cpuStarted, 0) / 1_000_000_000.0;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("benchmark", "java_protocol_kv");
        result.put("sdk_version", sdkVersion());
        result.put("java_version", System.getProperty("java.version"));
        result.put("transport", config.http() ? "http" : "native");
        result.put("url", sanitizedUrl(config.url()));
        result.put("http_version_requested", config.http() ? config.httpVersion() : null);
        result.put("preset", config.preset());
        result.put("command", config.command());
        result.put("test_time", config.durationSeconds());
        result.put("seconds", seconds);
        result.put("issued_requests", issued);
        result.put("completed_requests", completed);
        result.put("errors", errors);
        result.put("first_error", firstError);
        result.put("requests_per_sec", completed / seconds);
        result.put("client_cpu_seconds", cpuSeconds);
        result.put("client_cpu_percent", cpuSeconds / seconds * 100.0);
        result.put("client_instances", 1);
        result.put("native_socket_connections", config.http() ? null : 1);
        result.put("http_max_concurrent_requests", config.http() ? config.inflightBatches() : null);
        result.put("batch_size", config.batchSize());
        result.put("inflight_batches", config.inflightBatches());
        result.put("maximum_inflight_commands", config.batchSize() * config.inflightBatches());
        result.put("key_count", config.keyCount());
        result.put("value_bytes", config.valueBytes());
        result.put("warmed_keys", warmedKeys);
        result.put("batch_latency_samples", batchLatencies.size());
        result.put("batch_latency_avg_ms", average(batchLatencies));
        result.put("batch_latency_p50_ms", percentile(batchLatencies, 50));
        result.put("batch_latency_p95_ms", percentile(batchLatencies, 95));
        result.put("batch_latency_p99_ms", percentile(batchLatencies, 99));
        result.put(
                "batch_latency_max_ms",
                batchLatencies.stream().mapToDouble(Double::doubleValue).max().orElse(0.0));
        return result;
    }

    private static List<List<Object>> commandBatch(
            Config config, List<String> keys, byte[] value, long sequence, int count) {
        List<List<Object>> commands = new ArrayList<>(count);
        for (int offset = 0; offset < count; offset++) {
            long commandSequence = sequence + offset;
            String key = keys.get(Math.floorMod(commandSequence, keys.size()));
            commands.add(command(config.command(), key, value, commandSequence));
        }
        return commands;
    }

    static List<Object> command(String name, String key, byte[] value, long sequence) {
        return switch (name) {
            case "get" -> List.of("GET", key);
            case "set" -> List.of("SET", key, value);
            case "mixed" -> sequence % 2 == 0 ? List.of("GET", key) : List.of("SET", key, value);
            default -> throw new IllegalArgumentException("unsupported command: " + name);
        };
    }

    static double percentile(List<Double> values, int percentile) {
        if (values.isEmpty()) {
            return 0.0;
        }
        List<Double> ordered = new ArrayList<>(values);
        ordered.sort(Double::compareTo);
        int index = (int) Math.ceil(percentile / 100.0 * ordered.size()) - 1;
        return ordered.get(Math.max(0, Math.min(index, ordered.size() - 1)));
    }

    private static double average(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private static List<String> keys(String prefix, int count) {
        List<String> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(prefix + ':' + index);
        }
        return List.copyOf(result);
    }

    private static long secondsToNanos(double seconds) {
        return (long) (seconds * 1_000_000_000.0);
    }

    private static long processCpuNanos() {
        return ProcessHandle.current().info().totalCpuDuration().orElse(Duration.ZERO).toNanos();
    }

    private static String sdkVersion() {
        String version = FerricStoreClient.class.getPackage().getImplementationVersion();
        return version == null ? "development" : version;
    }

    private static String sanitizedUrl(String url) {
        return url.replaceFirst("(?<=://)[^/@]+@", "***@");
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName() + ": " + current.getMessage();
    }

    private record CompletedBatch(
            long startedNanos, int size, List<Object> responses, Throwable failure) {}

    private record WarmupBatch(CompletableFuture<List<Object>> future, int expected) {}

    record Config(
            String preset,
            String url,
            String command,
            double durationSeconds,
            int batchSize,
            int inflightBatches,
            int keyCount,
            int valueBytes,
            boolean warmup,
            String keyPrefix,
            String httpVersion) {
        static Config parse(String[] args) {
            Map<String, String> options = options(args);
            String preset = options.get("preset");
            Preset defaults = Preset.named(preset);
            String url = options.getOrDefault("url", DEFAULT_URL);
            String command = options.getOrDefault("command", defaults.command());
            double duration = decimal(options, "duration-seconds", defaults.durationSeconds());
            int batchSize = integer(options, "batch-size", defaults.batchSize());
            int inflight = integer(options, "inflight-batches", defaults.inflightBatches());
            int keyCount = integer(options, "key-count", 100_000);
            int valueBytes = integer(options, "value-bytes", 16);
            boolean warmup = !Boolean.parseBoolean(options.getOrDefault("no-warmup", "false"));
            String keyPrefix = options.getOrDefault("key-prefix", DEFAULT_PREFIX);
            String httpVersion = options.getOrDefault("http-version", "1.1");
            if (!List.of("get", "set", "mixed").contains(command)) {
                throw new IllegalArgumentException("--command must be get, set, or mixed");
            }
            if (duration <= 0.0) {
                throw new IllegalArgumentException("--duration-seconds must be positive");
            }
            positive(batchSize, "--batch-size");
            positive(inflight, "--inflight-batches");
            positive(keyCount, "--key-count");
            if (valueBytes < 0) {
                throw new IllegalArgumentException("--value-bytes must be non-negative");
            }
            if (keyPrefix.isBlank()) {
                throw new IllegalArgumentException("--key-prefix must not be blank");
            }
            if (!List.of("1.1", "2").contains(httpVersion)) {
                throw new IllegalArgumentException("--http-version must be 1.1 or 2");
            }
            return new Config(
                    preset,
                    url,
                    command,
                    duration,
                    batchSize,
                    inflight,
                    keyCount,
                    valueBytes,
                    warmup,
                    keyPrefix,
                    httpVersion);
        }

        boolean http() {
            return BenchmarkClients.http(url);
        }

        private static Map<String, String> options(String[] args) {
            Map<String, String> result = new LinkedHashMap<>();
            int index = 0;
            while (index < args.length) {
                String option = args[index];
                if ("--no-warmup".equals(option)) {
                    result.put("no-warmup", "true");
                    index++;
                    continue;
                }
                if (!option.startsWith("--") || index + 1 >= args.length) {
                    throw new IllegalArgumentException("invalid benchmark option: " + option);
                }
                String name = option.substring(2);
                if (!List.of(
                                "preset",
                                "url",
                                "command",
                                "duration-seconds",
                                "batch-size",
                                "inflight-batches",
                                "key-count",
                                "value-bytes",
                                "key-prefix",
                                "http-version")
                        .contains(name)) {
                    throw new IllegalArgumentException("unknown benchmark option: " + option);
                }
                result.put(name, args[index + 1]);
                index += 2;
            }
            return result;
        }

        private static int integer(Map<String, String> options, String name, int defaultValue) {
            try {
                return Integer.parseInt(options.getOrDefault(name, Integer.toString(defaultValue)));
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("--" + name + " must be an integer", error);
            }
        }

        private static double decimal(
                Map<String, String> options, String name, double defaultValue) {
            try {
                return Double.parseDouble(
                        options.getOrDefault(name, Double.toString(defaultValue)));
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("--" + name + " must be a number", error);
            }
        }

        private static void positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
        }
    }

    private record Preset(
            String command, int batchSize, int inflightBatches, double durationSeconds) {
        private static Preset named(String name) {
            if (name == null) {
                return new Preset("set", 50, 64, 10.0);
            }
            return switch (name) {
                case "get-throughput" -> new Preset("get", 1_000, 64, 30.0);
                case "set-throughput" -> new Preset("set", 500, 64, 30.0);
                case "get-latency" -> new Preset("get", 10, 8, 30.0);
                case "set-latency" -> new Preset("set", 10, 8, 30.0);
                case "mixed-throughput" -> new Preset("mixed", 500, 64, 30.0);
                default -> throw new IllegalArgumentException("unknown benchmark preset: " + name);
            };
        }
    }
}
