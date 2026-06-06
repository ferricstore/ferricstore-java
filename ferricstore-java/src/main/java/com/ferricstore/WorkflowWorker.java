package com.ferricstore;

import java.util.List;
import java.util.Map;

public final class WorkflowWorker {
    private final FerricStoreClient client;
    private final String type;
    private final String worker;
    private final List<String> states;
    private final Map<String, WorkflowHandler> handlers;

    WorkflowWorker(FerricStoreClient client, String type, String worker, List<String> states, Map<String, WorkflowHandler> handlers) {
        this.client = client;
        this.type = type;
        this.worker = worker;
        this.states = states;
        this.handlers = handlers;
    }

    public int runOnce() {
        int applied = 0;
        for (String state : states) {
            WorkflowHandler handler = handlers.get(state);
            if (handler == null) {
                throw new FerricStoreException("no workflow handler for state " + state);
            }
            List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder(type, worker).state(state).payload(true).limit(100).build());
            for (FlowRecord job : jobs) {
                apply(job, handler);
                applied++;
            }
        }
        return applied;
    }

    private void apply(FlowRecord job, WorkflowHandler handler) {
        try {
            Outcome outcome = handler.handle(new WorkflowContext(client, job));
            if (outcome instanceof TransitionOutcome transition) {
                client.transition(TransitionOptions.builder(job.id(), job.state(), transition.toState(), job.leaseToken(), job.fencingToken())
                    .partitionKey(job.partitionKey()).payload(transition.payload()).build());
            } else if (outcome instanceof CompleteOutcome complete) {
                client.complete(CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                    .partitionKey(job.partitionKey()).result(complete.result()).payload(complete.payload()).build());
            } else if (outcome instanceof RetryOutcome retry) {
                client.retry(RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                    .partitionKey(job.partitionKey()).error(retry.error()).payload(retry.payload()).build());
            } else if (outcome instanceof FailOutcome fail) {
                client.fail(FailOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                    .partitionKey(job.partitionKey()).error(fail.error()).payload(fail.payload()).build());
            }
        } catch (Exception e) {
            client.retry(RetryOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                .partitionKey(job.partitionKey()).error(errorPayload(e)).build());
        }
    }

    private static Map<String, String> errorPayload(Exception e) {
        return Map.of("message", String.valueOf(e.getMessage()), "type", e.getClass().getName());
    }
}
