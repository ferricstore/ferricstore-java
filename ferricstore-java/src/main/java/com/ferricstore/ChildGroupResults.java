package com.ferricstore;

import java.util.List;

/**
 * A parent's child group, with children ordered by id.
 *
 * @param waitMode {@code "all"}, {@code "any"}, or {@code "none"}
 * @param resolved {@code "success"} or {@code "failure"} once the group resolved, otherwise null
 */
public record ChildGroupResults(
        String groupId,
        String waitMode,
        String resolved,
        ChildGroupSummary summary,
        List<ChildResult> children) {
    public ChildGroupResults {
        children = ImmutableCopies.list(children);
    }
}
