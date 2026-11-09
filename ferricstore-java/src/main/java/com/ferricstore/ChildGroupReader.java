package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Reads one child group from the parent record, then every child value in one read. */
final class ChildGroupReader {
    private static final Set<String> STATUSES =
            Set.of("running", "completed", "failed", "cancelled");
    private static final Set<String> WAIT_MODES = Set.of("all", "any", "none");

    private ChildGroupReader() {}

    static ChildGroupResults read(
            FerricStoreClient client, Object parent, String groupId, Long maxBytes) {
        if (parent == null) {
            return null;
        }
        Object groups = Resp.map(parent).get("child_groups");
        if (groups == null) {
            return null;
        }
        Object rawGroup = Resp.map(groups).get(groupId);
        if (rawGroup == null) {
            return null;
        }
        Map<String, Object> group = Resp.map(rawGroup);
        String waitMode = Resp.string(group.get("wait"));
        if (!WAIT_MODES.contains(waitMode)) {
            throw new FerricStoreException(
                    "FLOW.GET child group returned unknown wait " + waitMode);
        }
        String resolved = Resp.optionalString(group.get("resolved"));
        if (resolved != null && !resolved.equals("success") && !resolved.equals("failure")) {
            throw new FerricStoreException(
                    "FLOW.GET child group returned unknown resolution " + resolved);
        }
        Map<String, Object> partitions = optionalMap(group.get("child_partitions"));
        Map<String, Object> terminal = optionalMap(group.get("results"));
        List<String> refs = new ArrayList<>();
        List<int[]> targets = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        List<String> statuses = new ArrayList<>();
        for (Map.Entry<String, Object> child :
                new TreeMap<>(optionalMap(group.get("children"))).entrySet()) {
            String status = Resp.string(child.getValue());
            if (!STATUSES.contains(status)) {
                throw new FerricStoreException(
                        "FLOW.GET child group returned unknown child status " + status);
            }
            Map<String, Object> stored = optionalMap(terminal.get(child.getKey()));
            for (int field = 0; field < 2; field++) {
                String ref =
                        Resp.optionalString(stored.get(field == 0 ? "result_ref" : "error_ref"));
                if (ref != null) {
                    refs.add(ref);
                    targets.add(new int[] {ids.size(), field});
                }
            }
            ids.add(child.getKey());
            statuses.add(status);
        }
        Object[][] values = new Object[ids.size()][2];
        List<Object> stored = client.valueMGet(refs, maxBytes);
        for (int index = 0; index < stored.size(); index++) {
            int[] target = targets.get(index);
            values[target[0]][target[1]] = stored.get(index);
        }
        List<ChildResult> children = new ArrayList<>(ids.size());
        for (int index = 0; index < ids.size(); index++) {
            String id = ids.get(index);
            children.add(
                    new ChildResult(
                            id,
                            Resp.optionalString(partitions.get(id)),
                            statuses.get(index),
                            values[index][0],
                            values[index][1]));
        }
        return new ChildGroupResults(groupId, waitMode, resolved, summary(group), children);
    }

    private static ChildGroupSummary summary(Map<String, Object> group) {
        Map<String, Object> counts = optionalMap(group.get("summary"));
        ChildGroupSummary summary =
                new ChildGroupSummary(
                        Resp.number(counts.get("total")),
                        Resp.number(counts.get("completed")),
                        Resp.number(counts.get("failed")),
                        Resp.number(counts.get("cancelled")));
        if (summary.total() < 0
                || summary.completed() < 0
                || summary.failed() < 0
                || summary.cancelled() < 0) {
            throw new FerricStoreException("FLOW.GET child group returned a negative count");
        }
        return summary;
    }

    private static Map<String, Object> optionalMap(Object value) {
        return value == null ? Map.of() : Resp.map(value);
    }
}
