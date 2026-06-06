package com.ferricstore.spring.statemachine;

import com.ferricstore.FerricStoreException;
import com.ferricstore.Outcome;
import com.ferricstore.Outcomes;
import com.ferricstore.WorkflowContext;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.StateMachineEventResult;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import reactor.core.publisher.Mono;

public final class FerricFlowStateMachine {
    private final StateMachineFactory<String, String> factory;
    private final Set<String> completeStates;
    private final Set<String> failStates;
    private final Set<String> retryStates;
    private final DeniedTransitionPolicy deniedPolicy;

    private FerricFlowStateMachine(Builder builder) {
        this.factory = builder.factory;
        this.completeStates = Set.copyOf(builder.completeStates);
        this.failStates = Set.copyOf(builder.failStates);
        this.retryStates = Set.copyOf(builder.retryStates);
        this.deniedPolicy = builder.deniedPolicy;
    }

    public static Builder builder(StateMachineFactory<String, String> factory) {
        return new Builder(factory);
    }

    public Outcome apply(WorkflowContext context, String event) {
        return apply(context, event, null);
    }

    public Outcome apply(WorkflowContext context, String event, Object terminalResult) {
        StateMachine<String, String> machine = factory.getStateMachine(context.id());
        restore(machine, context.state());

        Message<String> message =
                MessageBuilder.withPayload(event)
                        .setHeader(FerricStoreStateMachineHeaders.CLIENT, context.client())
                        .setHeader(FerricStoreStateMachineHeaders.WORKFLOW_CONTEXT, context)
                        .setHeader(FerricStoreStateMachineHeaders.FLOW_RECORD, context.job())
                        .build();

        List<StateMachineEventResult<String, String>> results =
                machine.sendEventCollect(Mono.just(message)).block();
        if (results == null
                || results.stream()
                        .noneMatch(
                                result ->
                                        result.getResultType()
                                                == StateMachineEventResult.ResultType.ACCEPTED)) {
            return denied(context, event);
        }

        String target = machine.getState().getId();
        if (target == null || target.equals(context.state())) {
            return denied(context, event);
        }
        if (completeStates.contains(target)) {
            return Outcomes.complete(
                    terminalResult == null ? Map.of("state", target) : terminalResult);
        }
        if (failStates.contains(target)) {
            return Outcomes.fail(Map.of("state", target, "event", event));
        }
        if (retryStates.contains(target)) {
            return Outcomes.retry(Map.of("state", target, "event", event));
        }
        return Outcomes.transition(target);
    }

    private void restore(StateMachine<String, String> machine, String state) {
        machine.stopReactively().block();
        DefaultStateMachineContext<String, String> context =
                new DefaultStateMachineContext<>(state, null, Map.of(), null);
        machine.getStateMachineAccessor()
                .doWithAllRegions(access -> access.resetStateMachineReactively(context).block());
        machine.startReactively().block();
    }

    private Outcome denied(WorkflowContext context, String event) {
        Map<String, Object> error =
                Map.of(
                        "message",
                        "Spring Statemachine denied transition",
                        "id",
                        context.id(),
                        "state",
                        context.state(),
                        "event",
                        event);
        return switch (deniedPolicy) {
            case FAIL -> Outcomes.fail(error);
            case RETRY -> Outcomes.retry(error);
            case THROW ->
                    throw new FerricStoreException(
                            "Spring Statemachine denied transition "
                                    + context.state()
                                    + " --"
                                    + event);
        };
    }

    public static final class Builder {
        private final StateMachineFactory<String, String> factory;
        private final Set<String> completeStates = new LinkedHashSet<>(Set.of("completed"));
        private final Set<String> failStates = new LinkedHashSet<>(Set.of("failed"));
        private final Set<String> retryStates = new LinkedHashSet<>();
        private DeniedTransitionPolicy deniedPolicy = DeniedTransitionPolicy.FAIL;

        private Builder(StateMachineFactory<String, String> factory) {
            if (factory == null) {
                throw new IllegalArgumentException("factory cannot be null");
            }
            this.factory = factory;
        }

        public Builder completeState(String state) {
            completeStates.add(requiredState(state));
            return this;
        }

        public Builder failState(String state) {
            failStates.add(requiredState(state));
            return this;
        }

        public Builder retryState(String state) {
            retryStates.add(requiredState(state));
            return this;
        }

        public Builder deniedPolicy(DeniedTransitionPolicy policy) {
            if (policy == null) {
                throw new IllegalArgumentException("policy cannot be null");
            }
            this.deniedPolicy = policy;
            return this;
        }

        public FerricFlowStateMachine build() {
            return new FerricFlowStateMachine(this);
        }

        private static String requiredState(String state) {
            if (state == null || state.isBlank()) {
                throw new IllegalArgumentException("state cannot be blank");
            }
            return state;
        }
    }
}
