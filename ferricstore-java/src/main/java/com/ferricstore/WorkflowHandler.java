package com.ferricstore;

@FunctionalInterface
public interface WorkflowHandler {
    Outcome handle(WorkflowContext context) throws Exception;
}
