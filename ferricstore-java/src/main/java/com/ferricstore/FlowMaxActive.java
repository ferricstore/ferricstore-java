package com.ferricstore;

import java.util.List;
import java.util.Map;

final class FlowMaxActive {
    private FlowMaxActive() {}

    static void append(List<Object> command, MaxActiveMs value) {
        if (value != null) {
            command.add("MAX_ACTIVE_MS");
            command.add(wireValue(value));
        }
    }

    static void put(Map<String, Object> item, MaxActiveMs value) {
        if (value != null) {
            item.put("max_active_ms", wireValue(value));
        }
    }

    static Object wireValue(MaxActiveMs value) {
        if (value instanceof MaxActiveMs.Finite finite) {
            return finite.milliseconds();
        }
        if (value instanceof MaxActiveMs.Infinity) {
            return "infinity";
        }
        throw new IllegalArgumentException("unsupported max_active_ms value");
    }
}
