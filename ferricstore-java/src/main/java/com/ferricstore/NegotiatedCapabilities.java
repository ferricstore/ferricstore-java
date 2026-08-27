package com.ferricstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** FerricStore 0.8 capabilities negotiated from the server's HELLO response. */
public record NegotiatedCapabilities(
        int maxResponseBytes, Map<Integer, String> compactResponseCodecs, boolean authRequired) {
    public NegotiatedCapabilities {
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
        compactResponseCodecs =
                Collections.unmodifiableMap(new LinkedHashMap<>(compactResponseCodecs));
    }
}
