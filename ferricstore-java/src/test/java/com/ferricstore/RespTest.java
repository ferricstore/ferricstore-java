package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class RespTest {
    @Test
    void rejectsMalformedNumericResponsesWithSdkErrors() {
        FerricStoreException integerError =
                assertThrows(FerricStoreException.class, () -> Resp.number("not-an-integer"));
        FerricStoreException decimalError =
                assertThrows(FerricStoreException.class, () -> Resp.decimal("not-a-decimal"));

        assertEquals("expected integer response, got: not-an-integer", integerError.getMessage());
        assertEquals("expected decimal response, got: not-a-decimal", decimalError.getMessage());
        assertEquals(1.25d, Resp.decimal("1.25"));
    }

    @Test
    void testMapRequiresCompleteKeyValuePairs() {
        assertThrows(IllegalArgumentException.class, () -> Resp.testMap("key"));
        assertEquals(1, Resp.testMap("key", "value").size());
    }
}
