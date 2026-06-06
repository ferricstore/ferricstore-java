package com.ferricstore;

import java.util.List;
import java.util.Map;

public final class QueueWorker {
    private final FerricStoreClient client;
    private final String type;
    private final String state;
    private final String worker;

    QueueWorker(FerricStoreClient client, String type, String state, String worker) {
        this.client = client;
        this.type = type;
        this.state = state;
        this.worker = worker;
    }

    public QueueWorkerResult runOnce(QueueHandler handler) {
        List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder(type, worker).state(state).payload(true).limit(100).build());
        int completed = 0;
        int retried = 0;
        int failed = 0;
        for (FlowRecord job : jobs) {
            try {
                Object result = handler.handle(job);
                Outcome outcome = result instanceof Outcome typed ? typed : Outcomes.complete(result);
                if (outcome instanceof CompleteOutcome complete) {
                    client.complete(CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                        .partitionKey(job.partitionKey()).result(complete.result()).payload(complete.payload()).build());
                    completed++;
                } else if (outcome instanceof RetryOutcome retry) {
                    client.retry(RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                        .partitionKey(job.partitionKey()).error(retry.error()).payload(retry.payload()).build());
                    retried++;
                } else if (outcome instanceof FailOutcome fail) {
                    client.fail(FailOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                        .partitionKey(job.partitionKey()).error(fail.error()).payload(fail.payload()).build());
                    failed++;
                } else {
                    throw new FerricStoreException("Queue handlers cannot return transition outcomes");
                }
            } catch (Exception e) {
                client.retry(RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                    .partitionKey(job.partitionKey()).error(errorPayload(e)).build());
                retried++;
            }
        }
        return new QueueWorkerResult(jobs.size(), completed, retried, failed);
    }

    private static Map<String, String> errorPayload(Exception e) {
        return Map.of("message", String.valueOf(e.getMessage()), "type", e.getClass().getName());
    }
}
