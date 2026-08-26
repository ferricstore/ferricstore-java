package com.ferricstore;

import java.util.List;
import java.util.Locale;
import java.util.Set;

final class NativeRouting {
    private static final Set<String> FLOW_MANY_MUTATIONS =
            Set.of(
                    "FLOW.COMPLETE_MANY",
                    "FLOW.TRANSITION_MANY",
                    "FLOW.RETRY_MANY",
                    "FLOW.FAIL_MANY",
                    "FLOW.CANCEL_MANY");

    private NativeRouting() {}

    static Object routeKey(List<Object> command) {
        if (command.size() < 2) {
            return command.get(0);
        }
        String name = Resp.string(command.get(0)).toUpperCase(Locale.ROOT);
        if ("FLOW.CLAIM_DUE".equals(name)) {
            Object worker = valueAfter(command, "WORKER");
            if (worker != null) {
                return worker;
            }
        }
        if (FLOW_MANY_MUTATIONS.contains(name)) {
            Object firstFlow = valueAfterLast(command, "ITEMS");
            if (firstFlow != null) {
                return firstFlow;
            }
        }
        if (name.startsWith("FLOW.EFFECT.")) {
            for (int index = 2; index + 1 < command.size(); index++) {
                if ("PARTITION".equalsIgnoreCase(Resp.string(command.get(index)))) {
                    return command.get(index + 1);
                }
            }
            return command.get(1);
        }
        return command.get(1);
    }

    private static Object valueAfter(List<Object> command, String option) {
        for (int index = 2; index + 1 < command.size(); index++) {
            if (option.equalsIgnoreCase(Resp.string(command.get(index)))) {
                return command.get(index + 1);
            }
        }
        return null;
    }

    private static Object valueAfterLast(List<Object> command, String option) {
        for (int index = command.size() - 2; index >= 2; index--) {
            if (option.equalsIgnoreCase(Resp.string(command.get(index)))) {
                return command.get(index + 1);
            }
        }
        return null;
    }
}
