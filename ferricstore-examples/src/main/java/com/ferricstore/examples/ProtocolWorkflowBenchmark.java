package com.ferricstore.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ferricstore.ClaimDueOptions;
import com.ferricstore.ClaimedItem;
import com.ferricstore.CompleteManyOptions;
import com.ferricstore.FencedItem;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.FlowRecord;
import com.ferricstore.TransitionManyOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Real-server state-machine workflow benchmark shaped like the Python async benchmark. */
public final class ProtocolWorkflowBenchmark {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DEFAULT_URL = "ferric://127.0.0.1:6388";
    private static final String INITIAL_STATE = "queued";

    private ProtocolWorkflowBenchmark() {}

    @SuppressWarnings("PMD.SystemPrintln") // JSON is the benchmark's machine-readable output.
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Map<String, Object> result = run(Config.parse(args));
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    static Map<String, Object> run(Config config) throws InterruptedException {
        String runId = "java-workflow-bench-" + UUID.randomUUID().toString().replace("-", "");
        String flowType = "workflow_bench:" + runId;
        List<String> states = workflowStates(config.steps());
        byte[] payload = new byte[config.payloadBytes()];
        java.util.Arrays.fill(payload, (byte) 'x');
        AtomicInteger completed = new AtomicInteger();
        AtomicLong claimedActions = new AtomicLong();
        long cpuStarted = processCpuNanos();
        long totalStarted = System.nanoTime();
        long deadline = totalStarted + secondsToNanos(config.timeoutSeconds());
        ExecutorService workerPool = Executors.newFixedThreadPool(config.workers());
        List<Future<WorkerResult>> workerFutures = List.of();
        long processStarted;
        long createStarted;
        CreationResult creation;
        long createFinished;

        try (WorkflowClients clients = WorkflowClients.connect(config)) {
            FerricStoreClient controlClient = clients.control();
            if ("live".equals(config.shape())) {
                processStarted = System.nanoTime();
                workerFutures =
                        startWorkers(
                                workerPool,
                                clients.workers(),
                                config,
                                runId,
                                flowType,
                                states,
                                completed,
                                claimedActions,
                                deadline);
                createStarted = System.nanoTime();
                creation = createWorkflows(clients.producers(), config, runId, flowType, payload);
                createFinished = System.nanoTime();
            } else {
                createStarted = System.nanoTime();
                creation = createWorkflows(clients.producers(), config, runId, flowType, payload);
                createFinished = System.nanoTime();
                processStarted = System.nanoTime();
                workerFutures =
                        startWorkers(
                                workerPool,
                                clients.workers(),
                                config,
                                runId,
                                flowType,
                                states,
                                completed,
                                claimedActions,
                                deadline);
            }

            List<WorkerResult> workers = awaitWorkers(workerFutures);
            long processFinished = System.nanoTime();
            validateCounts(config, creation.created(), completed.get(), claimedActions.get());
            int verificationErrors = verifyWorkflows(controlClient, config, runId);
            if (verificationErrors != 0) {
                throw new IllegalStateException(
                        "workflow verification failed for " + verificationErrors + " samples");
            }
            long cpuFinished = processCpuNanos();
            return metrics(
                    config,
                    flowType,
                    states,
                    creation,
                    workers,
                    completed.get(),
                    claimedActions.get(),
                    verificationErrors,
                    totalStarted,
                    createStarted,
                    createFinished,
                    processStarted,
                    processFinished,
                    cpuStarted,
                    cpuFinished);
        } finally {
            workerFutures.forEach(future -> future.cancel(true));
            workerPool.shutdownNow();
            workerPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static CreationResult createWorkflows(
            List<FerricStoreClient> clients,
            Config config,
            String runId,
            String flowType,
            byte[] payload)
            throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(clients.size());
        List<Future<CreationResult>> futures = new ArrayList<>(clients.size());
        try {
            for (int producer = 0; producer < clients.size(); producer++) {
                int producerIndex = producer;
                FerricStoreClient client = clients.get(producer);
                futures.add(
                        producers.submit(
                                () ->
                                        createProducerWorkflows(
                                                client,
                                                config,
                                                runId,
                                                flowType,
                                                payload,
                                                producerIndex)));
            }
            int created = 0;
            List<Double> latencies = new ArrayList<>();
            for (Future<CreationResult> future : futures) {
                try {
                    CreationResult result = future.get();
                    created += result.created();
                    latencies.addAll(result.latencies());
                } catch (ExecutionException error) {
                    futures.forEach(item -> item.cancel(true));
                    throw benchmarkFailure("workflow producer failed", error);
                }
            }
            return new CreationResult(created, latencies);
        } finally {
            futures.forEach(future -> future.cancel(true));
            producers.shutdownNow();
            producers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings(
            "PMD.AvoidInstantiatingObjectsInLoops") // Each wire batch needs its own snapshot.
    private static CreationResult createProducerWorkflows(
            FerricStoreClient client,
            Config config,
            String runId,
            String flowType,
            byte[] payload,
            int producerIndex)
            throws InterruptedException {
        LinkedBlockingQueue<CompletedCreateBatch> completions = new LinkedBlockingQueue<>();
        List<Double> latencies = new ArrayList<>();
        int nextIndex = producerIndex;
        int created = 0;
        int pending = 0;
        while (nextIndex < config.flows() || pending > 0) {
            while (nextIndex < config.flows() && pending < config.createInflight()) {
                long now = System.currentTimeMillis();
                List<List<Object>> commands = new ArrayList<>(config.createBatchSize());
                while (nextIndex < config.flows() && commands.size() < config.createBatchSize()) {
                    int index = nextIndex;
                    commands.add(
                            createCommand(
                                    runId,
                                    flowType,
                                    index,
                                    INITIAL_STATE,
                                    partitionKey(config, runId, index),
                                    payload,
                                    now));
                    nextIndex += config.producers();
                }
                long started = System.nanoTime();
                CompletableFuture<List<Object>> future = client.pipelineAsync(commands);
                int count = commands.size();
                future.whenComplete(
                        (responses, failure) ->
                                completions.add(
                                        new CompletedCreateBatch(
                                                started, count, responses, failure)));
                pending++;
            }
            CompletedCreateBatch batch = completions.take();
            pending--;
            latencies.add(elapsedMillis(batch.startedNanos()));
            if (batch.failure() != null) {
                throw benchmarkFailure("workflow create batch failed", batch.failure());
            }
            if (batch.responses() == null || batch.responses().size() != batch.count()) {
                throw new IllegalStateException(
                        "workflow create batch returned incomplete results");
            }
            created += batch.count();
        }
        return new CreationResult(created, latencies);
    }

    static List<Object> createCommand(
            String runId,
            String flowType,
            int index,
            String state,
            String partitionKey,
            byte[] payload,
            long now) {
        List<Object> command =
                new ArrayList<>(
                        List.of(
                                "FLOW.CREATE",
                                flowId(runId, index),
                                "TYPE",
                                flowType,
                                "STATE",
                                state,
                                "NOW",
                                now));
        if (partitionKey != null) {
            command.add("PARTITION");
            command.add(partitionKey);
        }
        if (payload.length > 0) {
            command.add("PAYLOAD");
            command.add(payload);
        }
        command.add("RUN_AT");
        command.add(now);
        command.add("PRIORITY");
        command.add(0);
        return command;
    }

    private static List<Future<WorkerResult>> startWorkers(
            ExecutorService pool,
            List<WorkerClients> clients,
            Config config,
            String runId,
            String flowType,
            List<String> states,
            AtomicInteger completed,
            AtomicLong claimedActions,
            long deadline) {
        List<Future<WorkerResult>> workers = new ArrayList<>(config.workers());
        for (int worker = 0; worker < config.workers(); worker++) {
            int workerIndex = worker;
            workers.add(
                    pool.submit(
                            () ->
                                    runWorker(
                                            clients.get(workerIndex),
                                            config,
                                            runId,
                                            flowType,
                                            states,
                                            workerIndex,
                                            completed,
                                            claimedActions,
                                            deadline)));
        }
        return List.copyOf(workers);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // Each claim owns fresh fencing data.
    private static WorkerResult runWorker(
            WorkerClients clients,
            Config config,
            String runId,
            String flowType,
            List<String> states,
            int workerIndex,
            AtomicInteger completed,
            AtomicLong claimedActions,
            long deadline)
            throws InterruptedException {
        List<String> partitions = ownedPartitions(config, runId, workerIndex);
        if ("explicit".equals(config.partitionMode()) && partitions.isEmpty()) {
            return WorkerResult.empty();
        }
        List<Double> claimLatencies = new ArrayList<>();
        List<Double> applyLatencies = new ArrayList<>();
        long claimCalls = 0;
        long emptyClaims = 0;
        int maxClaimBatch = 0;
        int stateCursor = workerIndex % states.size();
        long idleMillis = config.idleSleepMs();

        while (completed.get() < config.flows()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("workflow benchmark exceeded its deadline");
            }
            String state = states.get(stateCursor);
            stateCursor = (stateCursor + 1) % states.size();
            long claimStarted = System.nanoTime();
            List<ClaimedItem> items =
                    clients.claim()
                            .claimJobs(
                                    claimOptions(
                                            flowType,
                                            runId + ":worker:" + workerIndex,
                                            state,
                                            partitions,
                                            config.leaseMs(),
                                            config.claimBatchSize()));
            claimLatencies.add(elapsedMillis(claimStarted));
            claimCalls++;
            if (items.isEmpty()) {
                emptyClaims++;
                if (idleMillis > 0) {
                    Thread.sleep(idleMillis);
                    idleMillis = Math.min(config.maxIdleSleepMs(), Math.max(1, idleMillis * 2));
                }
                continue;
            }

            idleMillis = config.idleSleepMs();
            maxClaimBatch = Math.max(maxClaimBatch, items.size());
            claimedActions.addAndGet(items.size());
            long applyStarted = System.nanoTime();
            if (state.equals(states.get(states.size() - 1))) {
                clients.apply()
                        .completeMany(
                                CompleteManyOptions.builder(items)
                                        .independent("independent".equals(config.mutationMode()))
                                        .returnOkOnSuccess(true)
                                        .build());
                completed.addAndGet(items.size());
            } else {
                List<FencedItem> fenced =
                        items.stream()
                                .map(
                                        item ->
                                                new FencedItem(
                                                        item.id(),
                                                        item.fencingToken(),
                                                        item.leaseToken(),
                                                        item.partitionKey()))
                                .toList();
                String nextState = states.get(states.indexOf(state) + 1);
                clients.apply()
                        .transitionMany(
                                TransitionManyOptions.builder("running", nextState, fenced)
                                        .independent("independent".equals(config.mutationMode()))
                                        .returnOkOnSuccess(true)
                                        .build());
            }
            applyLatencies.add(elapsedMillis(applyStarted));
        }
        return new WorkerResult(
                claimCalls, emptyClaims, maxClaimBatch, claimLatencies, applyLatencies);
    }

    static ClaimDueOptions claimOptions(
            String type,
            String worker,
            String state,
            List<String> partitions,
            long leaseMs,
            int limit) {
        ClaimDueOptions.Builder claim =
                ClaimDueOptions.builder(type, worker)
                        .state(state)
                        .leaseMs(leaseMs)
                        .limit(limit)
                        .priority(0)
                        .reclaimExpired(false);
        if (!partitions.isEmpty()) {
            claim.partitionKeys(partitions);
        }
        return claim.build();
    }

    private static List<String> ownedPartitions(Config config, String runId, int workerIndex) {
        if ("auto".equals(config.partitionMode())) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (int partition = workerIndex;
                partition < config.partitions();
                partition += config.workers()) {
            result.add(runId + ":partition:" + partition);
        }
        return List.copyOf(result);
    }

    private static List<WorkerResult> awaitWorkers(List<Future<WorkerResult>> workers)
            throws InterruptedException {
        List<WorkerResult> results = new ArrayList<>(workers.size());
        for (Future<WorkerResult> worker : workers) {
            try {
                results.add(worker.get());
            } catch (ExecutionException error) {
                workers.forEach(future -> future.cancel(true));
                throw benchmarkFailure("workflow worker failed", error);
            }
        }
        return List.copyOf(results);
    }

    private static int verifyWorkflows(FerricStoreClient client, Config config, String runId) {
        int samples = Math.min(config.verifySamples(), config.flows());
        int errors = 0;
        for (int sample = 0; sample < samples; sample++) {
            int index =
                    samples == 1
                            ? config.flows() - 1
                            : sample * (config.flows() - 1) / (samples - 1);
            FlowRecord record =
                    client.get(flowId(runId, index), partitionKey(config, runId, index));
            if (record == null || !"completed".equals(record.state())) {
                errors++;
            }
        }
        return errors;
    }

    private static void validateCounts(
            Config config, int created, int completed, long claimedActions) {
        long expectedActions = (long) config.flows() * config.steps();
        if (created != config.flows()) {
            throw new IllegalStateException(
                    "created " + created + " workflows; expected " + config.flows());
        }
        if (completed != config.flows()) {
            throw new IllegalStateException(
                    "completed " + completed + " workflows; expected " + config.flows());
        }
        if (claimedActions != expectedActions) {
            throw new IllegalStateException(
                    "claimed " + claimedActions + " actions; expected " + expectedActions);
        }
    }

    @SuppressWarnings("PMD.NcssCount") // Keep the stable JSON result schema in one audit point.
    private static Map<String, Object> metrics(
            Config config,
            String flowType,
            List<String> states,
            CreationResult creation,
            List<WorkerResult> workers,
            int completed,
            long claimedActions,
            int verificationErrors,
            long totalStarted,
            long createStarted,
            long createFinished,
            long processStarted,
            long processFinished,
            long cpuStarted,
            long cpuFinished) {
        double createSeconds = secondsBetween(createStarted, createFinished);
        double processSeconds = secondsBetween(processStarted, processFinished);
        double totalSeconds = secondsBetween(totalStarted, processFinished);
        List<Double> claimLatencies = new ArrayList<>();
        List<Double> applyLatencies = new ArrayList<>();
        long claimCalls = 0;
        long emptyClaims = 0;
        int maxClaimBatch = 0;
        for (WorkerResult worker : workers) {
            claimCalls += worker.claimCalls();
            emptyClaims += worker.emptyClaims();
            maxClaimBatch = Math.max(maxClaimBatch, worker.maxClaimBatch());
            claimLatencies.addAll(worker.claimLatencies());
            applyLatencies.addAll(worker.applyLatencies());
        }
        double cpuSeconds = Math.max(cpuFinished - cpuStarted, 0) / 1_000_000_000.0;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("benchmark", "java_protocol_workflow");
        result.put("sdk_version", sdkVersion());
        result.put("java_version", System.getProperty("java.version"));
        result.put("transport", BenchmarkClients.http(config.url()) ? "http" : "native");
        result.put("url", sanitizedUrl(config.url()));
        result.put(
                "http_version_requested",
                BenchmarkClients.http(config.url()) ? config.httpVersion() : null);
        result.put(
                "http_format_requested",
                BenchmarkClients.http(config.url()) ? config.httpFormat() : null);
        result.put("shape", config.shape());
        result.put("flow_type", flowType);
        result.put("flows", config.flows());
        result.put("steps", config.steps());
        result.put("states", states);
        result.put("created", creation.created());
        result.put("completed", completed);
        result.put("claimed_actions", claimedActions);
        result.put("expected_actions", (long) config.flows() * config.steps());
        result.put("errors", 0);
        result.put("verification_samples", Math.min(config.verifySamples(), config.flows()));
        result.put("verification_errors", verificationErrors);
        result.put("workers", config.workers());
        result.put("producers", config.producers());
        result.put("producer_connection_mode", config.producerConnectionMode());
        result.put("worker_connection_mode", config.workerConnectionMode());
        result.put("mutation_mode", config.mutationMode());
        result.put(
                "client_instances",
                1
                        + ("dedicated".equals(config.producerConnectionMode())
                                ? config.producers()
                                : 0)
                        + ("dedicated".equals(config.workerConnectionMode())
                                ? config.workers() * 2
                                : 0));
        result.put("partitions", config.partitions());
        result.put("partition_mode", config.partitionMode());
        result.put("create_batch_size", config.createBatchSize());
        result.put("create_inflight", config.createInflight());
        result.put("claim_batch_size", config.claimBatchSize());
        result.put("payload_bytes", config.payloadBytes());
        result.put("claim_calls", claimCalls);
        result.put("empty_claims", emptyClaims);
        result.put("empty_claim_ratio", claimCalls == 0 ? 0.0 : (double) emptyClaims / claimCalls);
        result.put("avg_claim_batch", claimCalls == 0 ? 0.0 : (double) claimedActions / claimCalls);
        result.put("max_claim_batch", maxClaimBatch);
        result.put("create_seconds", createSeconds);
        result.put("process_seconds", processSeconds);
        result.put("total_seconds", totalSeconds);
        result.put("create_flows_per_sec", creation.created() / createSeconds);
        result.put("workflow_completions_per_sec", completed / processSeconds);
        result.put("state_actions_per_sec", claimedActions / processSeconds);
        result.put("end_to_end_workflows_per_sec", completed / totalSeconds);
        result.put("client_cpu_seconds", cpuSeconds);
        result.put("client_cpu_percent", cpuSeconds / totalSeconds * 100.0);
        addLatencyMetrics(result, "create_batch", creation.latencies());
        addLatencyMetrics(result, "claim", claimLatencies);
        addLatencyMetrics(result, "apply", applyLatencies);
        return result;
    }

    private static void addLatencyMetrics(
            Map<String, Object> result, String prefix, List<Double> latencies) {
        result.put(prefix + "_latency_samples", latencies.size());
        result.put(prefix + "_latency_p50_ms", ProtocolKvBenchmark.percentile(latencies, 50));
        result.put(prefix + "_latency_p95_ms", ProtocolKvBenchmark.percentile(latencies, 95));
        result.put(prefix + "_latency_p99_ms", ProtocolKvBenchmark.percentile(latencies, 99));
    }

    static List<String> workflowStates(int steps) {
        List<String> states = new ArrayList<>(steps);
        states.add(INITIAL_STATE);
        for (int step = 1; step < steps; step++) {
            states.add("step_" + step);
        }
        return List.copyOf(states);
    }

    private static String flowId(String runId, int index) {
        return runId + ":flow:" + index;
    }

    private static String partitionKey(Config config, String runId, int index) {
        return "auto".equals(config.partitionMode())
                ? null
                : runId + ":partition:" + index % config.partitions();
    }

    private static RuntimeException benchmarkFailure(String message, Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return new IllegalStateException(message + ": " + root.getMessage(), root);
    }

    private static double elapsedMillis(long started) {
        return (System.nanoTime() - started) / 1_000_000.0;
    }

    private static double secondsBetween(long started, long finished) {
        return Math.max((finished - started) / 1_000_000_000.0, 1.0e-9);
    }

    private static long secondsToNanos(long seconds) {
        try {
            return Duration.ofSeconds(seconds).toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
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

    private record CompletedCreateBatch(
            long startedNanos, int count, List<Object> responses, Throwable failure) {}

    private record CreationResult(int created, List<Double> latencies) {}

    private record WorkerResult(
            long claimCalls,
            long emptyClaims,
            int maxClaimBatch,
            List<Double> claimLatencies,
            List<Double> applyLatencies) {
        private static WorkerResult empty() {
            return new WorkerResult(0, 0, 0, List.of(), List.of());
        }
    }

    private record WorkerClients(FerricStoreClient claim, FerricStoreClient apply) {}

    private record WorkflowClients(
            FerricStoreClient control,
            List<FerricStoreClient> producers,
            List<WorkerClients> workers,
            List<FerricStoreClient> owned)
            implements AutoCloseable {
        private static WorkflowClients connect(Config config) {
            List<FerricStoreClient> owned = new ArrayList<>();
            try {
                FerricStoreClient control = connectOne(config, 1);
                owned.add(control);
                List<FerricStoreClient> producers = new ArrayList<>(config.producers());
                for (int index = 0; index < config.producers(); index++) {
                    FerricStoreClient producer =
                            "shared".equals(config.producerConnectionMode())
                                    ? control
                                    : connectOne(config, config.createInflight());
                    producers.add(producer);
                    if ("dedicated".equals(config.producerConnectionMode())) {
                        owned.add(producer);
                    }
                }
                List<WorkerClients> workers = new ArrayList<>(config.workers());
                for (int index = 0; index < config.workers(); index++) {
                    if ("shared".equals(config.workerConnectionMode())) {
                        workers.add(new WorkerClients(control, control));
                    } else {
                        FerricStoreClient claim = connectOne(config, 1);
                        FerricStoreClient apply = connectOne(config, 2);
                        workers.add(new WorkerClients(claim, apply));
                        owned.add(claim);
                        owned.add(apply);
                    }
                }
                return new WorkflowClients(
                        control, List.copyOf(producers), List.copyOf(workers), List.copyOf(owned));
            } catch (RuntimeException failure) {
                closeAll(owned);
                throw failure;
            }
        }

        private static FerricStoreClient connectOne(Config config, int maxConcurrentRequests) {
            return BenchmarkClients.connect(
                    config.url(),
                    config.httpVersion(),
                    maxConcurrentRequests,
                    config.createBatchSize(),
                    "msgpack".equals(config.httpFormat()));
        }

        @Override
        public void close() {
            closeAll(owned);
        }

        private static void closeAll(List<FerricStoreClient> clients) {
            for (int index = clients.size() - 1; index >= 0; index--) {
                clients.get(index).close();
            }
        }
    }

    record Config(
            String url,
            String shape,
            int flows,
            int steps,
            int workers,
            int producers,
            int partitions,
            String partitionMode,
            String producerConnectionMode,
            String workerConnectionMode,
            String mutationMode,
            int createBatchSize,
            int createInflight,
            int claimBatchSize,
            int payloadBytes,
            long leaseMs,
            long idleSleepMs,
            long maxIdleSleepMs,
            long timeoutSeconds,
            int verifySamples,
            String httpVersion,
            String httpFormat) {
        static Config parse(String[] args) {
            Map<String, String> options = options(args);
            String url = options.getOrDefault("url", DEFAULT_URL);
            String shape = options.getOrDefault("shape", "live");
            int flows = integer(options, "flows", 10_000);
            int steps = integer(options, "steps", 3);
            int workers = integer(options, "workers", 16);
            int producers = integer(options, "producers", 4);
            int partitions = integer(options, "partitions", 16);
            String partitionMode = options.getOrDefault("partition-mode", "auto");
            String producerConnectionMode =
                    options.getOrDefault("producer-connection-mode", "dedicated");
            String workerConnectionMode =
                    options.getOrDefault("worker-connection-mode", "dedicated");
            String mutationMode = options.getOrDefault("mutation-mode", "atomic");
            int createBatchSize = integer(options, "create-batch-size", 500);
            int createInflight = integer(options, "create-inflight", 32);
            int claimBatchSize = integer(options, "claim-batch-size", 500);
            int payloadBytes = integer(options, "payload-bytes", 0);
            long leaseMs = integer(options, "lease-ms", 30_000);
            long idleSleepMs = integer(options, "idle-sleep-ms", 1);
            long maxIdleSleepMs = integer(options, "max-idle-sleep-ms", 10);
            long timeoutSeconds = integer(options, "timeout-seconds", 120);
            int verifySamples = integer(options, "verify-samples", 100);
            String httpVersion = options.getOrDefault("http-version", "1.1");
            String httpFormat = options.getOrDefault("http-format", "json");
            if (!List.of("live", "preloaded").contains(shape)) {
                throw new IllegalArgumentException("--shape must be live or preloaded");
            }
            if (!List.of("auto", "explicit").contains(partitionMode)) {
                throw new IllegalArgumentException("--partition-mode must be auto or explicit");
            }
            if (!List.of("dedicated", "shared").contains(workerConnectionMode)) {
                throw new IllegalArgumentException(
                        "--worker-connection-mode must be dedicated or shared");
            }
            if (!List.of("dedicated", "shared").contains(producerConnectionMode)) {
                throw new IllegalArgumentException(
                        "--producer-connection-mode must be dedicated or shared");
            }
            if (!List.of("atomic", "independent").contains(mutationMode)) {
                throw new IllegalArgumentException("--mutation-mode must be atomic or independent");
            }
            positive(flows, "--flows");
            positive(steps, "--steps");
            positive(workers, "--workers");
            positive(producers, "--producers");
            positive(partitions, "--partitions");
            positive(createBatchSize, "--create-batch-size");
            positive(createInflight, "--create-inflight");
            positive(claimBatchSize, "--claim-batch-size");
            positive((int) leaseMs, "--lease-ms");
            positive((int) timeoutSeconds, "--timeout-seconds");
            if (payloadBytes < 0 || idleSleepMs < 0 || maxIdleSleepMs < idleSleepMs) {
                throw new IllegalArgumentException("workflow sizes and idle delays are invalid");
            }
            if (verifySamples < 0) {
                throw new IllegalArgumentException("--verify-samples must be non-negative");
            }
            if (!List.of("1.1", "2").contains(httpVersion)) {
                throw new IllegalArgumentException("--http-version must be 1.1 or 2");
            }
            if (!List.of("json", "msgpack").contains(httpFormat)) {
                throw new IllegalArgumentException("--http-format must be json or msgpack");
            }
            return new Config(
                    url,
                    shape,
                    flows,
                    steps,
                    workers,
                    producers,
                    partitions,
                    partitionMode,
                    producerConnectionMode,
                    workerConnectionMode,
                    mutationMode,
                    createBatchSize,
                    createInflight,
                    claimBatchSize,
                    payloadBytes,
                    leaseMs,
                    idleSleepMs,
                    maxIdleSleepMs,
                    timeoutSeconds,
                    verifySamples,
                    httpVersion,
                    httpFormat);
        }

        private static Map<String, String> options(String[] args) {
            List<String> names =
                    List.of(
                            "url",
                            "shape",
                            "flows",
                            "steps",
                            "workers",
                            "producers",
                            "partitions",
                            "partition-mode",
                            "producer-connection-mode",
                            "worker-connection-mode",
                            "mutation-mode",
                            "create-batch-size",
                            "create-inflight",
                            "claim-batch-size",
                            "payload-bytes",
                            "lease-ms",
                            "idle-sleep-ms",
                            "max-idle-sleep-ms",
                            "timeout-seconds",
                            "verify-samples",
                            "http-version",
                            "http-format");
            Map<String, String> result = new LinkedHashMap<>();
            for (int index = 0; index < args.length; index += 2) {
                if (!args[index].startsWith("--") || index + 1 >= args.length) {
                    throw new IllegalArgumentException("invalid benchmark option: " + args[index]);
                }
                String name = args[index].substring(2);
                if (!names.contains(name)) {
                    throw new IllegalArgumentException("unknown benchmark option: " + args[index]);
                }
                result.put(name, args[index + 1]);
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

        private static void positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
        }
    }
}
