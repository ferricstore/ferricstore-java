package com.ferricstore;

import java.util.Map;

/** Common claim view shared by full workflow records and compact claimed items. */
public interface ClaimedFlow {
    /** Returns the workflow ID. */
    String id();

    /** Returns the opaque token for the current lease. */
    String leaseToken();

    /** Returns the monotonic fencing token for the current claim. */
    long fencingToken();

    /** Returns the workflow partition key. */
    String partitionKey();

    /** Returns the workflow type. */
    String type();

    /** Returns the physical lifecycle state, such as {@code running}. */
    String state();

    /** Returns the application's logical workflow state within the active claim. */
    String runState();

    /** Returns the workflow payload. */
    Object payload();

    /** Returns the workflow attributes. */
    Map<String, Object> attributes();
}
