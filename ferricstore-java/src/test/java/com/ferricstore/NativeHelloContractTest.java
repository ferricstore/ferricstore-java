package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class NativeHelloContractTest {
    @Test
    void consumesTheServerOwnedCompactCodecTableAndResponseLimit() {
        NegotiatedCapabilities capabilities =
                NativeHelloContract.parse(
                        Map.of(
                                "protocol", "ferricstore-native",
                                "version", 1L,
                                "auth_required", true,
                                "capabilities",
                                        Map.of(
                                                "limits", Map.of("max_response_bytes", 4096L),
                                                "response_codecs",
                                                        Map.of(
                                                                "compact_response_opcodes",
                                                                Map.of(
                                                                        "kv_mget_v1",
                                                                                List.of(0x0104L),
                                                                        "flow_value_mget_v2",
                                                                                List.of(0x020CL))))));

        assertEquals(4096, capabilities.maxResponseBytes());
        assertEquals("kv_mget_v1", capabilities.compactResponseCodecs().get(0x0104));
        assertEquals("flow_value_mget_v2", capabilities.compactResponseCodecs().get(0x020C));
        assertEquals(true, capabilities.authRequired());
    }

    @Test
    void rejectsPre08HelloShapesInsteadOfUsingALegacyCapabilityTable() {
        Map<String, Object> legacy =
                Map.of(
                        "protocol", "ferricstore-native",
                        "version", 1L,
                        "capabilities", Map.of("limits", Map.of("max_response_bytes", 4096L)));

        NativeProtocolException error =
                assertThrows(
                        NativeProtocolException.class, () -> NativeHelloContract.parse(legacy));
        assertEquals(
                "FerricStore server does not satisfy the minimum 0.8.0 HELLO contract: "
                        + "HELLO is missing response_codecs",
                error.getMessage());
    }
}
