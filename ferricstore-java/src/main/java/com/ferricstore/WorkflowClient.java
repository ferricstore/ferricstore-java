package com.ferricstore;

public final class WorkflowClient {
    private final FerricStoreClient client;

    public WorkflowClient(FerricStoreClient client) {
        this.client = client;
    }

    public Workflow workflow(String type, String initialState) {
        return new Workflow(client, type, initialState == null ? "queued" : initialState);
    }
}
