package com.ferricstore;

public final class WorkflowContext {
    private final FerricStoreClient client;
    private final FlowRecord job;

    WorkflowContext(FerricStoreClient client, FlowRecord job) {
        this.client = client;
        this.job = job;
    }

    public FerricStoreClient client() {
        return client;
    }

    public FlowRecord job() {
        return job;
    }

    public String id() {
        return job.id();
    }

    public Object payload() {
        return job.payload();
    }

    public String state() {
        return job.state();
    }

    public String partitionKey() {
        return job.partitionKey();
    }

    public Object value(String name) {
        Object direct = job.values().get(name);
        if (direct != null) {
            return direct;
        }
        Object ref = job.valueRefs().get(name);
        if (ref == null) {
            return null;
        }
        return client.valueMGet(java.util.List.of(Resp.string(ref))).stream()
                .findFirst()
                .orElse(null);
    }
}
