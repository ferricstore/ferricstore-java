package com.ferricstore;

import java.util.concurrent.CompletionStage;

/** A non-blocking workflow handler used by {@link Workflow#stateAsync}. */
@FunctionalInterface
public interface AsyncWorkflowHandler {
    /**
     * Handles one claimed workflow without blocking the worker thread.
     *
     * @param context the claimed workflow context
     * @return the outcome to persist after the asynchronous work completes
     * @throws Exception if the handler cannot be started
     */
    CompletionStage<Outcome> handle(WorkflowContext context) throws Exception;
}
