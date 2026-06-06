package com.ferricstore.spring.statemachine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.RedisExecutor;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.StateMachineFactory;

final class FerricFlowStateMachineTest {
    @Test
    void springStateMachineValidatesTransitionWhileFerricStorePersistsState() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "created")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine = FerricFlowStateMachine.builder(orderMachine()).build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("created", context -> machine.apply(context, "CHARGE"));

        int applied = workflow.worker("worker-1", List.of("created")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.CLAIM_DUE"));
        assertEquals(1, executor.count("SET"));
        assertEquals(1, executor.count("FLOW.TRANSITION"));
        List<Object> transition = executor.first("FLOW.TRANSITION");
        assertEquals("order-1", transition.get(1));
        assertEquals("created", transition.get(2));
        assertEquals("charged", transition.get(3));
    }

    @Test
    void terminalStateMapsToFerricFlowComplete() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "charged")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine = FerricFlowStateMachine.builder(orderMachine()).build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state(
                                "charged",
                                context -> machine.apply(context, "COMPLETE", Map.of("ok", true)));

        int applied = workflow.worker("worker-1", List.of("charged")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.COMPLETE"));
        assertTrue(executor.first("FLOW.COMPLETE").contains("RESULT"));
    }

    @Test
    void deniedTransitionCanFailWithoutUsingSpringPersistence() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "created")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine())
                        .deniedPolicy(DeniedTransitionPolicy.FAIL)
                        .build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("created", context -> machine.apply(context, "UNKNOWN"));

        int applied = workflow.worker("worker-1", List.of("created")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.FAIL"));
    }

    @Test
    void customFailStateMapsToFerricFlowFail() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "charged")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine()).failState("rejected").build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("charged", context -> machine.apply(context, "REJECT"));

        int applied = workflow.worker("worker-1", List.of("charged")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.FAIL"));
    }

    @Test
    void customRetryStateMapsToFerricFlowRetry() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "charged")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine()).retryState("waiting").build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("charged", context -> machine.apply(context, "WAIT"));

        int applied = workflow.worker("worker-1", List.of("charged")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.RETRY"));
    }

    @Test
    void customCompleteStateMapsToFerricFlowComplete() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "charged")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine()).completeState("archived").build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("charged", context -> machine.apply(context, "ARCHIVE"));

        int applied = workflow.worker("worker-1", List.of("charged")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.COMPLETE"));
        assertTrue(executor.first("FLOW.COMPLETE").contains("RESULT"));
    }

    @Test
    void deniedTransitionCanRetryWithoutUsingSpringPersistence() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "created")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine())
                        .deniedPolicy(DeniedTransitionPolicy.RETRY)
                        .build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("created", context -> machine.apply(context, "UNKNOWN"));

        int applied = workflow.worker("worker-1", List.of("created")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.RETRY"));
    }

    @Test
    void deniedTransitionCanThrowAndWorkerRetries() throws Exception {
        CapturingExecutor executor =
                new CapturingExecutor(List.of(flowRecord("order-1", "created")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor, new JsonCodec());
        FerricFlowStateMachine machine =
                FerricFlowStateMachine.builder(orderMachine())
                        .deniedPolicy(DeniedTransitionPolicy.THROW)
                        .build();
        Workflow workflow =
                new WorkflowClient(client)
                        .workflow("order", "created")
                        .state("created", context -> machine.apply(context, "UNKNOWN"));

        int applied = workflow.worker("worker-1", List.of("created")).runOnce();

        assertEquals(1, applied);
        assertEquals(1, executor.count("FLOW.RETRY"));
        assertEquals(0, executor.count("FLOW.FAIL"));
    }

    @Test
    void builderRejectsInvalidConfiguration() throws Exception {
        StateMachineFactory<String, String> factory = orderMachine();

        assertThrows(IllegalArgumentException.class, () -> FerricFlowStateMachine.builder(null));
        assertThrows(
                IllegalArgumentException.class,
                () -> FerricFlowStateMachine.builder(factory).completeState(""));
        assertThrows(
                IllegalArgumentException.class,
                () -> FerricFlowStateMachine.builder(factory).failState(" "));
        assertThrows(
                IllegalArgumentException.class,
                () -> FerricFlowStateMachine.builder(factory).retryState(null));
        assertThrows(
                IllegalArgumentException.class,
                () -> FerricFlowStateMachine.builder(factory).deniedPolicy(null));
    }

    private static StateMachineFactory<String, String> orderMachine() throws Exception {
        StateMachineBuilder.Builder<String, String> builder = StateMachineBuilder.builder();
        builder.configureStates()
                .withStates()
                .initial("created")
                .state("charged")
                .state("waiting")
                .end("completed")
                .end("failed")
                .end("rejected")
                .end("archived");
        builder.configureTransitions()
                .withExternal()
                .source("created")
                .target("charged")
                .event("CHARGE")
                .action(
                        context ->
                                FerricStoreStateMachineContext.client(context)
                                        .kv()
                                        .set(
                                                "order:"
                                                        + FerricStoreStateMachineContext.flowRecord(
                                                                        context)
                                                                .id(),
                                                Map.of("charged", true)))
                .and()
                .withExternal()
                .source("charged")
                .target("completed")
                .event("COMPLETE")
                .and()
                .withExternal()
                .source("charged")
                .target("rejected")
                .event("REJECT")
                .and()
                .withExternal()
                .source("charged")
                .target("waiting")
                .event("WAIT")
                .and()
                .withExternal()
                .source("charged")
                .target("archived")
                .event("ARCHIVE");
        return builder.createFactory();
    }

    private static Map<String, Object> flowRecord(String id, String state) {
        return Map.of(
                "id",
                id,
                "type",
                "order",
                "state",
                state,
                "partition_key",
                "p1",
                "lease_token",
                "lease-" + id,
                "fencing_token",
                1L,
                "version",
                1L);
    }

    private static final class CapturingExecutor implements RedisExecutor {
        private final Object claimResponse;
        private final List<List<Object>> calls = Collections.synchronizedList(new ArrayList<>());

        private CapturingExecutor(Object claimResponse) {
            this.claimResponse = claimResponse;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.getFirst())) {
                return claimResponse;
            }
            return "OK";
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.getFirst())).count();
            }
        }

        private List<Object> first(String command) {
            synchronized (calls) {
                return calls.stream()
                        .filter(call -> command.equals(call.getFirst()))
                        .findFirst()
                        .orElseThrow();
            }
        }
    }
}
