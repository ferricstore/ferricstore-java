package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import com.ferricstore.WorkflowContext;
import com.ferricstore.spring.statemachine.FerricFlowStateMachine;
import com.ferricstore.spring.statemachine.FerricStoreStateMachineContext;
import java.util.List;
import java.util.Map;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.StateMachineFactory;

public final class SpringStateMachineWorkflowExample {
    private SpringStateMachineWorkflowExample() {}

    public static void main(String[] args) throws Exception {
        try (FerricStoreClient client =
                FerricStoreClient.connect("redis://127.0.0.1:6379/0", new JsonCodec())) {
            FerricFlowStateMachine graph = FerricFlowStateMachine.builder(orderMachine()).build();
            Workflow order =
                    new WorkflowClient(client)
                            .workflow("order", "created")
                            .state("created", context -> graph.apply(context, "CHARGE"))
                            .state(
                                    "charged",
                                    context ->
                                            graph.apply(context, "COMPLETE", Map.of("ok", true)));

            order.start("order-1", Map.of("amount", 42, "userId", "user-1"));
            order.worker("order-worker-1", List.of("created", "charged"))
                    .concurrency(64)
                    .virtualThreads()
                    .runOnce();
        }
    }

    private static StateMachineFactory<String, String> orderMachine() throws Exception {
        StateMachineBuilder.Builder<String, String> builder = StateMachineBuilder.builder();
        builder.configureStates().withStates().initial("created").state("charged").end("completed");
        builder.configureTransitions()
                .withExternal()
                .source("created")
                .target("charged")
                .event("CHARGE")
                .action(
                        context -> {
                            WorkflowContext workflow =
                                    FerricStoreStateMachineContext.workflowContext(context);
                            FerricStoreStateMachineContext.client(context)
                                    .kv()
                                    .set("order:" + workflow.id(), Map.of("charged", true));
                        })
                .and()
                .withExternal()
                .source("charged")
                .target("completed")
                .event("COMPLETE");
        return builder.createFactory();
    }
}
