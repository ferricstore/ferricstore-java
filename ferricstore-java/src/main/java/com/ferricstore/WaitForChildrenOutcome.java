package com.ferricstore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates child Flows and parks the parent in {@code waitState} in one fenced command.
 *
 * <p>FerricStore moves the parent to {@code success} once the group resolves, or to {@code failure}
 * at the first failed child unless {@code onChildFailed} is {@code "ignore"}. Read the children
 * with {@link WorkflowContext#childResults(String)}. Return this outcome instead of calling {@code
 * spawnChildren} and then transitioning: a fast child can finish before that transition commits.
 * Fan-in needs FerricStore 0.11.19 or newer.
 */
public record WaitForChildrenOutcome(
        List<ChildSpec> children,
        String waitState,
        String success,
        String failure,
        String groupId,
        String waitMode,
        String onChildFailed,
        String onParentClosed,
        MaxActiveMs maxActiveMs,
        Map<String, ?> values,
        Map<String, String> valueRefs)
        implements Outcome {
    public WaitForChildrenOutcome {
        children = ImmutableCopies.list(children);
        values = ImmutableCopies.map(values);
        valueRefs = ImmutableCopies.map(valueRefs);
        if (children.isEmpty()) {
            throw new IllegalArgumentException("waitForChildren requires at least one child");
        }
        requireState("waitState", waitState);
        requireState("success", success);
        requireState("failure", failure);
        if (groupId == null || groupId.isBlank()) {
            groupId = "default";
        }
        if (waitMode == null || waitMode.isBlank()) {
            waitMode = "all";
        }
        if (!waitMode.equals("all") && !waitMode.equals("any")) {
            throw new IllegalArgumentException("waitForChildren waitMode must be all or any");
        }
    }

    public static Builder builder(
            List<ChildSpec> children, String waitState, String success, String failure) {
        return new Builder(children, waitState, success, failure);
    }

    private static void requireState(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("waitForChildren requires " + name);
        }
    }

    SpawnChildrenOptions spawnOptions(ClaimedItem claim) {
        SpawnChildrenOptions.Builder builder =
                SpawnChildrenOptions.builder(claim.id(), children)
                        .partitionKey(claim.partitionKey())
                        .leaseToken(claim.leaseToken())
                        .fencingToken(claim.fencingToken())
                        .groupId(groupId)
                        .waitMode(waitMode)
                        .waitState(waitState)
                        .success(success)
                        .failure(failure)
                        .onChildFailed(onChildFailed)
                        .onParentClosed(onParentClosed)
                        .maxActiveMs(maxActiveMs)
                        .values(values)
                        .valueRefs(valueRefs);
        return builder.build();
    }

    public static final class Builder {
        private final List<ChildSpec> children;
        private final String waitState;
        private final String success;
        private final String failure;
        private String groupId = "default";
        private String waitMode = "all";
        private String onChildFailed;
        private String onParentClosed;
        private MaxActiveMs maxActiveMs;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();

        private Builder(
                List<ChildSpec> children, String waitState, String success, String failure) {
            this.children = children;
            this.waitState = waitState;
            this.success = success;
            this.failure = failure;
        }

        public Builder groupId(String value) {
            this.groupId = value;
            return this;
        }

        /** {@code "all"} (the default) or {@code "any"}. */
        public Builder waitMode(String value) {
            this.waitMode = value;
            return this;
        }

        /** {@code "fail_parent"} (the default) or {@code "ignore"}. */
        public Builder onChildFailed(String value) {
            this.onChildFailed = value;
            return this;
        }

        /** {@code "cancel_children"} (the default) or {@code "abandon_children"}. */
        public Builder onParentClosed(String value) {
            this.onParentClosed = value;
            return this;
        }

        public Builder maxActiveMs(MaxActiveMs value) {
            this.maxActiveMs = value;
            return this;
        }

        public Builder values(Map<String, ?> value) {
            this.values.putAll(value);
            return this;
        }

        public Builder valueRefs(Map<String, String> value) {
            this.valueRefs.putAll(value);
            return this;
        }

        public WaitForChildrenOutcome build() {
            return new WaitForChildrenOutcome(
                    children,
                    waitState,
                    success,
                    failure,
                    groupId,
                    waitMode,
                    onChildFailed,
                    onParentClosed,
                    maxActiveMs,
                    values,
                    valueRefs);
        }
    }
}
