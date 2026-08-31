package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Workflow {
    private final FerricStoreClient client;
    private final String type;
    private final String initialState;
    private final Map<String, WorkflowHandler> handlers = new LinkedHashMap<>();
    private final Map<String, AsyncWorkflowHandler> asyncHandlers = new LinkedHashMap<>();

    Workflow(FerricStoreClient client, String type, String initialState) {
        this.client = client;
        this.type = type;
        this.initialState = initialState;
    }

    public Workflow state(String name, WorkflowHandler handler) {
        handlers.put(name, handler);
        asyncHandlers.remove(name);
        return this;
    }

    /**
     * Registers a CompletionStage-based handler without requiring Reactor or another runtime.
     *
     * @param name the logical workflow state handled by the callback
     * @param handler the non-blocking callback for that state
     * @return this workflow definition
     */
    public Workflow stateAsync(String name, AsyncWorkflowHandler handler) {
        asyncHandlers.put(name, handler);
        handlers.remove(name);
        return this;
    }

    public Object start(String id, Object payload) {
        return client.create(
                CreateOptions.builder(id, type)
                        .state(initialState)
                        .payload(payload)
                        .idempotent(true)
                        .build());
    }

    public WorkflowWorker worker(String worker, List<String> states) {
        return new WorkflowWorker(
                client,
                type,
                worker,
                states == null ? registeredStates() : states,
                handlers,
                asyncHandlers);
    }

    private List<String> registeredStates() {
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>(handlers.keySet());
        names.addAll(asyncHandlers.keySet());
        return List.copyOf(names);
    }
}
