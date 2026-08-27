package com.ferricstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A FerricStore command error returned inside a successful HTTP batch response. */
public final class HttpCommandException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    private final String errorCode;
    private final boolean retryable;
    private final boolean safeToRetry;
    private final Long retryAfterMs;
    private final transient Map<String, Object> raw;

    HttpCommandException(
            String message,
            String errorCode,
            boolean retryable,
            boolean safeToRetry,
            Long retryAfterMs,
            Map<String, Object> raw) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.safeToRetry = safeToRetry;
        this.retryAfterMs = retryAfterMs;
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
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
