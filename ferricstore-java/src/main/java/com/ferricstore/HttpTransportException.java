package com.ferricstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A top-level HTTP transport or endpoint failure where no command result was returned. */
public final class HttpTransportException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    private final int statusCode;
    private final String errorCode;
    private final boolean retryable;
    private final boolean safeToRetry;
    private final Long retryAfterMs;
    private final transient Map<String, Object> raw;

    HttpTransportException(
            String message,
            int statusCode,
            String errorCode,
            boolean retryable,
            boolean safeToRetry,
            Long retryAfterMs,
            Map<String, Object> raw,
            Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.safeToRetry = safeToRetry;
        this.retryAfterMs = retryAfterMs;
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
    }

    public int statusCode() {
        return statusCode;
    }

    public String errorCode() {
        return errorCode;
    }

    public boolean retryable() {
        return retryable;
    }

    public boolean safeToRetry() {
        return safeToRetry;
    }

    public Long retryAfterMs() {
        return retryAfterMs;
    }

    public Map<String, Object> raw() {
        return raw == null ? Map.of() : raw;
    }
}
