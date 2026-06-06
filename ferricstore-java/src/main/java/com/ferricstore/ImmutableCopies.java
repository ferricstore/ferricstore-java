package com.ferricstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ImmutableCopies {
    private ImmutableCopies() {}

    static <T> List<T> list(List<? extends T> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    static <K, V> Map<K, V> map(Map<? extends K, ? extends V> values) {
        return values == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
