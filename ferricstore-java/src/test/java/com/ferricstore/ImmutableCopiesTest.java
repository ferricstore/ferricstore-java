package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class ImmutableCopiesTest {
    @Test
    void canonicalizesEmptyCollectionsWithoutWeakeningImmutability() {
        List<Object> list = ImmutableCopies.list(new ArrayList<>());
        Map<Object, Object> map = ImmutableCopies.map(new LinkedHashMap<>());

        assertTrue(list.isEmpty());
        assertTrue(map.isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> list.add("value"));
        assertThrows(UnsupportedOperationException.class, () -> map.put("key", "value"));
    }

    @Test
    void stillMakesDefensiveCopiesOfNonEmptyCollections() {
        List<String> sourceList = new ArrayList<>(List.of("first"));
        Map<String, String> sourceMap = new LinkedHashMap<>(Map.of("key", "first"));

        List<String> list = ImmutableCopies.list(sourceList);
        Map<String, String> map = ImmutableCopies.map(sourceMap);
        sourceList.set(0, "changed");
        sourceMap.put("key", "changed");

        assertEquals(List.of("first"), list);
        assertEquals(Map.of("key", "first"), map);
    }
}
