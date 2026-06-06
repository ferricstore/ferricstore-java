package com.ferricstore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class QueueWorker {
    private final FerricStoreClient client;
    private final String type;
    private final String state;
    private final String worker;
    private final int batchSize;
    private final int concurrency;
    private final boolean virtualThreads;
    private final ExecutorService executor;

    QueueWorker(FerricStoreClient client, String type, String state, String worker) {
        this(client, type, state, worker, WorkerExecutors.DEFAULT_BATCH_SIZE, 1, false, null);
    }

    private QueueWorker(
            FerricStoreClient client,
            String type,
            String state,
            String worker,
            int batchSize,
            int concurrency,
            boolean virtualThreads,
            ExecutorService executor) {
        this.client = client;
        this.type = type;
        this.state = state;
        this.worker = worker;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.virtualThreads = virtualThreads;
        this.executor = executor;
    }

    public QueueWorker batchSize(int batchSize) {
        WorkerExecutors.requirePositive("batchSize", batchSize);
        return new QueueWorker(
                client, type, state, worker, batchSize, concurrency, virtualThreads, executor);
    }

    public QueueWorker concurrency(int concurrency) {
        WorkerExecutors.requirePositive("concurrency", concurrency);
        return new QueueWorker(
                client, type, state, worker, batchSize, concurrency, virtualThreads, executor);
    }

    public QueueWorker virtualThreads() {
        return new QueueWorker(client, type, state, worker, batchSize, concurrency, true, null);
    }

    public QueueWorker executor(ExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        return new QueueWorker(
                client, type, state, worker, batchSize, concurrency, false, executor);
    }

    public QueueWorkerResult runOnce(QueueHandler handler) {
        List<FlowRecord> jobs =
                client.claimDue(
                        ClaimDueOptions.builder(type, worker)
                                .state(state)
                                .payload(true)
                                .limit(batchSize)
                                .build());
        List<JobResult> results =
                WorkerExecutors.run(
                        jobs, concurrency, virtualThreads, executor, job -> apply(job, handler));
        return new QueueWorkerResult(
                jobs.size(),
                count(results, JobResult.COMPLETED),
                count(results, JobResult.RETRIED),
                count(results, JobResult.FAILED));
    }

    private JobResult apply(FlowRecord job, QueueHandler handler) {
        try {
            Object result = handler.handle(job);
            Outcome outcome = result instanceof Outcome typed ? typed : Outcomes.complete(result);
            if (outcome instanceof CompleteOutcome complete) {
                client.complete(
                        CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .result(complete.result())
                                .payload(complete.payload())
                                .build());
                return JobResult.COMPLETED;
            }
            if (outcome instanceof RetryOutcome retry) {
                client.retry(
                        RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .error(retry.error())
                                .payload(retry.payload())
                                .build());
                return JobResult.RETRIED;
            }
            if (outcome instanceof FailOutcome fail) {
                client.fail(
                        FailOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .error(fail.error())
                                .payload(fail.payload())
                                .build());
                return JobResult.FAILED;
            }
            throw new FerricStoreException("Queue handlers cannot return transition outcomes");
        } catch (Exception e) {
            client.retry(
                    RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                            .partitionKey(job.partitionKey())
                            .error(errorPayload(e))
                            .build());
            return JobResult.RETRIED;
        }
    }

    private static int count(List<JobResult> results, JobResult expected) {
        return (int) results.stream().filter(result -> result == expected).count();
    }

    private static Map<String, String> errorPayload(Exception e) {
        return Map.of("message", String.valueOf(e.getMessage()), "type", e.getClass().getName());
    }

    private enum JobResult {
        COMPLETED,
        RETRIED,
        FAILED
    }
}
