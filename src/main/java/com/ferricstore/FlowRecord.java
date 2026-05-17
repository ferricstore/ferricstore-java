package com.ferricstore;

import java.util.Map;

public record FlowRecord(
    String id,
    String type,
    String state,
    String partitionKey,
    byte[] payload,
    String leaseToken,
    long fencingToken,
    long version,
    String parentFlowId,
    String rootFlowId,
    String correlationId,
    Map<String, Object> raw
) {
}
