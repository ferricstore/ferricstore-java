package com.ferricstore;

import java.util.Map;

public record RateLimitResult(String status, long count, long remaining, long resetMs, boolean allowed, Map<String, Object> raw) {
}
