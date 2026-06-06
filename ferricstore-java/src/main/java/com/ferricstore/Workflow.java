package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Workflow {
    private final FerricStoreClient client;
    private final String type;
    private final String initialState;
    private final Map<String, WorkflowHandler> handlers = new LinkedHashMap<>();

    Workflow(FerricStoreClient client, String type, String initialState) {
        this.client = client;
        this.type = type;
        this.initialState = initialState;
    }

    public Workflow state(String name, WorkflowHandler handler) {
        handlers.put(name, handler);
        return this;
    }

    public Object start(String id, Object payload) {
        return client.create(CreateOptions.builder(id, type).state(initialState).payload(payload).idempotent(true).build());
    }

    public WorkflowWorker worker(String worker, List<String> states) {
        return new WorkflowWorker(client, type, worker, states == null ? List.copyOf(handlers.keySet()) : states, handlers);
    }
}
