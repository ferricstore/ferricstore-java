package com.ferricstore.spring.statemachine;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.FlowRecord;
import com.ferricstore.WorkflowContext;
import org.springframework.statemachine.StateContext;

public final class FerricStoreStateMachineContext {
    private FerricStoreStateMachineContext() {}

    public static FerricStoreClient client(StateContext<String, String> context) {
        return header(context, FerricStoreStateMachineHeaders.CLIENT, FerricStoreClient.class);
    }

    public static WorkflowContext workflowContext(StateContext<String, String> context) {
        return header(
                context, FerricStoreStateMachineHeaders.WORKFLOW_CONTEXT, WorkflowContext.class);
    }

    public static FlowRecord flowRecord(StateContext<String, String> context) {
        return header(context, FerricStoreStateMachineHeaders.FLOW_RECORD, FlowRecord.class);
    }

    private static <T> T header(StateContext<String, String> context, String name, Class<T> type) {
        Object value = context.getMessageHeader(name);
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        throw new IllegalStateException("missing Spring Statemachine header " + name);
    }
}
