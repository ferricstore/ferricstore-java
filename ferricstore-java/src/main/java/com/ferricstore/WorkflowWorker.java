package com.ferricstore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class WorkflowWorker {
    private final FerricStoreClient client;
    private final String type;
    private final String worker;
    private final List<String> states;
    private final Map<String, WorkflowHandler> handlers;
    private final int batchSize;
    private final int concurrency;
    private final boolean virtualThreads;
    private final ExecutorService executor;

    WorkflowWorker(
            FerricStoreClient client,
            String type,
            String worker,
            List<String> states,
            Map<String, WorkflowHandler> handlers) {
        this(
                client,
                type,
                worker,
                states,
                handlers,
                WorkerExecutors.DEFAULT_BATCH_SIZE,
                1,
                false,
                null);
    }

    private WorkflowWorker(
            FerricStoreClient client,
            String type,
            String worker,
            List<String> states,
            Map<String, WorkflowHandler> handlers,
            int batchSize,
            int concurrency,
            boolean virtualThreads,
            ExecutorService executor) {
        this.client = client;
        this.type = type;
        this.worker = worker;
        this.states = states;
        this.handlers = handlers;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.virtualThreads = virtualThreads;
        this.executor = executor;
    }

    public WorkflowWorker batchSize(int batchSize) {
        WorkerExecutors.requirePositive("batchSize", batchSize);
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor);
    }

    public WorkflowWorker concurrency(int concurrency) {
        WorkerExecutors.requirePositive("concurrency", concurrency);
        return new WorkflowWorker(
                client,
                type,
                worker,
                states,
                handlers,
                batchSize,
                concurrency,
                virtualThreads,
                executor);
    }

    public WorkflowWorker virtualThreads() {
        return new WorkflowWorker(
                client, type, worker, states, handlers, batchSize, concurrency, true, null);
    }

    public WorkflowWorker executor(ExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        return new WorkflowWorker(
                client, type, worker, states, handlers, batchSize, concurrency, false, executor);
    }

    public int runOnce() {
        int applied = 0;
        for (String state : states) {
            WorkflowHandler handler = handlers.get(state);
            if (handler == null) {
                throw new FerricStoreException("no workflow handler for state " + state);
            }
            List<FlowRecord> jobs =
                    client.claimDue(
                            ClaimDueOptions.builder(type, worker)
                                    .state(state)
                                    .payload(true)
                                    .limit(batchSize)
                                    .build());
            applied +=
                    WorkerExecutors.run(
                                    jobs,
                                    concurrency,
                                    virtualThreads,
                                    executor,
                                    job -> apply(job, handler))
                            .size();
        }
        return applied;
    }

    private Void apply(FlowRecord job, WorkflowHandler handler) {
        try {
            Outcome outcome = handler.handle(new WorkflowContext(client, job));
            if (outcome instanceof TransitionOutcome transition) {
                client.transition(
                        TransitionOptions.builder(
                                        job.id(),
                                        job.state(),
                                        transition.toState(),
                                        job.leaseToken(),
                                        job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .payload(transition.payload())
                                .build());
            } else if (outcome instanceof CompleteOutcome complete) {
                client.complete(
                        CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .result(complete.result())
                                .payload(complete.payload())
                                .build());
            } else if (outcome instanceof RetryOutcome retry) {
                client.retry(
                        RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .error(retry.error())
                                .payload(retry.payload())
                                .build());
            } else if (outcome instanceof FailOutcome fail) {
                client.fail(
                        FailOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                                .partitionKey(job.partitionKey())
                                .error(fail.error())
                                .payload(fail.payload())
                                .build());
            }
            return null;
        } catch (Exception e) {
            client.retry(
                    RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                            .partitionKey(job.partitionKey())
                            .error(errorPayload(e))
                            .build());
            return null;
        }
    }

    private static Map<String, String> errorPayload(Exception e) {
        return Map.of("message", String.valueOf(e.getMessage()), "type", e.getClass().getName());
    }
}
