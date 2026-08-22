package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;

final class CommandTestSupport {
    private CommandTestSupport() {}

    static void assertCommand(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size(), "argument count");
        for (int index = 0; index < expected.size(); index++) {
            Object wanted = expected.get(index);
            Object got = actual.get(index);
            if (wanted instanceof byte[] wantedBytes && got instanceof byte[] gotBytes) {
                assertArrayEquals(wantedBytes, gotBytes, "argument " + index);
            } else {
                assertEquals(wanted, got, "argument " + index);
            }
        }
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
