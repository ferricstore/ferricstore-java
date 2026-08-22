package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** A structured non-success response returned by the FerricStore native server. */
public final class NativeServerException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    private final int status;
    private final Object raw;
    private final Boolean retryable;
    private final Boolean safeToRetry;
    private final Long retryAfterMs;

    NativeServerException(int status, Object raw) {
        super(message(status, raw));
        this.status = status;
        this.raw = raw;
        Map<String, Object> fields = fields(raw);
        this.retryable = optionalBoolean(fields.get("retryable"));
        this.safeToRetry = optionalBoolean(fields.get("safe_to_retry"));
        this.retryAfterMs = optionalLong(fields.get("retry_after_ms"));
    }

    public int status() {
        return status;
    }

    public Object raw() {
        return raw;
    }

    public Boolean retryable() {
        return retryable;
    }

    public Boolean safeToRetry() {
        return safeToRetry;
    }

    public Long retryAfterMs() {
        return retryAfterMs;
    }

    private static String message(int status, Object raw) {
        Object detail = fields(raw).get("message");
        String text = text(detail == null ? raw : detail);
        return "FerricStore native " + statusName(status) + (text.isEmpty() ? "" : ": " + text);
    }

    private static String statusName(int status) {
        return switch (status) {
            case NativeProtocol.STATUS_ERROR -> "error";
            case NativeProtocol.STATUS_AUTH -> "authentication error";
            case NativeProtocol.STATUS_NOPERM -> "permission error";
            case NativeProtocol.STATUS_BUSY -> "busy";
            case NativeProtocol.STATUS_REROUTE -> "reroute";
            case NativeProtocol.STATUS_BAD_REQUEST -> "bad request";
            default -> "status " + status;
        };
    }

    private static Map<String, Object> fields(Object raw) {
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, value) -> result.put(text(key), value));
        return result;
    }

    private static Boolean optionalBoolean(Object value) {
        return value instanceof Boolean flag ? flag : null;
    }

    private static Long optionalLong(Object value) {
        return value instanceof Byte
                        || value instanceof Short
                        || value instanceof Integer
                        || value instanceof Long
                ? ((Number) value).longValue()
                : null;
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value == null ? "" : String.valueOf(value);
    }
}
