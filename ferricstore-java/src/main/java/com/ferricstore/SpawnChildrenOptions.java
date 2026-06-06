package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record SpawnChildrenOptions(
    String parentId,
    List<ChildSpec> children,
    String partitionKey,
    String leaseToken,
    Long fencingToken,
    String groupId,
    String waitMode,
    String waitState,
    String success,
    String failure,
    String fromState,
    String onChildFailed,
    String onParentClosed,
    Map<String, ?> values,
    Map<String, String> valueRefs,
    long nowMs
) {
    public static Builder builder(String parentId, List<ChildSpec> children) {
        return new Builder(parentId, children);
    }

    public static final class Builder {
        private final String parentId;
        private final List<ChildSpec> children;
        private String partitionKey;
        private String leaseToken;
        private Long fencingToken;
        private String groupId = "default";
        private String waitMode = "all";
        private String waitState;
        private String success;
        private String failure;
        private String fromState;
        private String onChildFailed;
        private String onParentClosed;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private long nowMs;

        private Builder(String parentId, List<ChildSpec> children) {
            this.parentId = parentId;
            this.children = List.copyOf(children);
        }

        public Builder partitionKey(String value) { this.partitionKey = value; return this; }
        public Builder leaseToken(String value) { this.leaseToken = value; return this; }
        public Builder fencingToken(long value) { this.fencingToken = value; return this; }
        public Builder groupId(String value) { this.groupId = value; return this; }
        public Builder waitMode(String value) { this.waitMode = value; return this; }
        public Builder waitState(String value) { this.waitState = value; return this; }
        public Builder success(String value) { this.success = value; return this; }
        public Builder failure(String value) { this.failure = value; return this; }
        public Builder fromState(String value) { this.fromState = value; return this; }
        public Builder onChildFailed(String value) { this.onChildFailed = value; return this; }
        public Builder onParentClosed(String value) { this.onParentClosed = value; return this; }
        public Builder value(String name, Object value) { this.values.put(name, value); return this; }
        public Builder values(Map<String, ?> values) { this.values.putAll(values); return this; }
        public Builder valueRef(String name, String ref) { this.valueRefs.put(name, ref); return this; }
        public Builder valueRefs(Map<String, String> valueRefs) { this.valueRefs.putAll(valueRefs); return this; }
        public Builder nowMs(long value) { this.nowMs = value; return this; }

        public SpawnChildrenOptions build() {
            return new SpawnChildrenOptions(parentId, children, partitionKey, leaseToken,
                fencingToken, groupId, waitMode, waitState, success, failure, fromState,
                onChildFailed, onParentClosed, Map.copyOf(values), Map.copyOf(valueRefs), nowMs);
        }
    }
}
