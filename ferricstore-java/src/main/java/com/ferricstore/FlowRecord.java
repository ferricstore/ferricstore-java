package com.ferricstore;

import java.util.Map;

public record FlowRecord(
    String id,
    String type,
    String state,
    String partitionKey,
    Object payload,
    String leaseToken,
    long fencingToken,
    long version,
    String parentFlowId,
    String rootFlowId,
    String correlationId,
    Map<String, Object> values,
    Map<String, Object> valueRefs,
    Map<String, Object> raw
) {
    public <T> T payloadAs(Class<T> type) {
        return type.cast(payload);
    }
}
