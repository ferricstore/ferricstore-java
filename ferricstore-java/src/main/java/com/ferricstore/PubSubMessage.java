package com.ferricstore;

import java.util.Map;

/** One native Pub/Sub delivery. */
public record PubSubMessage(
        String kind, String channel, Object message, String pattern, Map<String, Object> raw) {
    public PubSubMessage {
        raw = ImmutableCopies.map(raw);
    }
}
