package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
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

    @Test
    void compactClaimRowsRemainOrderedAndUnmodifiable() {
        List<ClaimedItem> items =
                Resp.claimedItems(
                        List.of(
                                List.of("a", "p1", "lease-a", 1L),
                                List.of("b", "p2", "lease-b", 2L)));

        assertEquals(List.of("a", "b"), items.stream().map(ClaimedItem::id).toList());
        assertThrows(
                UnsupportedOperationException.class,
                () -> items.add(new ClaimedItem("c", "lease-c", 3L, "p3")));
    }
}
