package com.ferricstore;

import java.util.List;
import java.util.Locale;

final class NativeRouting {
    private NativeRouting() {}

    static Object routeKey(List<Object> command) {
        if (command.size() < 2) {
            return command.getFirst();
        }
        String name = Resp.string(command.getFirst()).toUpperCase(Locale.ROOT);
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
}
