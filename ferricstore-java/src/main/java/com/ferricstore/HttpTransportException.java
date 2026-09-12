package com.ferricstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A top-level HTTP transport or endpoint failure where no command result was returned. */
public final class HttpTransportException extends FerricStoreException
        implements RequestDeliveryFailure {
    private static final long serialVersionUID = 1L;

    private final int statusCode;
    private final String errorCode;
    private final boolean retryable;
    private final boolean safeToRetry;
    private final Long retryAfterMs;
    private final transient Map<String, Object> raw;
    private final RequestDelivery delivery;

    HttpTransportException(
            String message,
            int statusCode,
            String errorCode,
            boolean retryable,
            boolean safeToRetry,
            Long retryAfterMs,
            Map<String, Object> raw,
            Throwable cause) {
        this(
                message,
                statusCode,
                errorCode,
                retryable,
                safeToRetry,
                retryAfterMs,
                raw,
                cause,
                inferredDelivery(statusCode, errorCode, safeToRetry));
    }

    HttpTransportException(
            String message,
            int statusCode,
            String errorCode,
            boolean retryable,
            boolean safeToRetry,
            Long retryAfterMs,
            Map<String, Object> raw,
            Throwable cause,
            RequestDelivery delivery) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.safeToRetry = safeToRetry;
        this.retryAfterMs = retryAfterMs;
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
        this.delivery = delivery;
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

    @Override
    public RequestDelivery delivery() {
        return delivery;
    }

    private static RequestDelivery inferredDelivery(
            int statusCode, String errorCode, boolean safeToRetry) {
        if (statusCode == 408 || "request_timeout".equals(errorCode)) {
            return RequestDelivery.UNKNOWN;
        }
        if (safeToRetry || definitelyRejectedStatus(statusCode)) {
            return RequestDelivery.REJECTED;
        }
        return RequestDelivery.UNKNOWN;
    }

    private static boolean definitelyRejectedStatus(int statusCode) {
        return switch (statusCode) {
            case 400, 401, 403, 404, 405, 406, 411, 413, 414, 415, 422, 426, 431 -> true;
            default -> false;
        };
    }
}
