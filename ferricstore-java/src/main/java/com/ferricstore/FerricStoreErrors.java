package com.ferricstore;

import java.util.Locale;

public final class FerricStoreErrors {
    private FerricStoreErrors() {}

    static RuntimeException map(Throwable error) {
        String message = error.getMessage() == null ? error.toString() : error.getMessage();
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("flow already exists")) {
            return new FlowAlreadyExistsException(message, error);
        }
        if (lower.contains("flow not found")) {
            return new FlowNotFoundException(message, error);
        }
        if (lower.contains("stale") && lower.contains("lease")) {
            return new StaleLeaseException(message, error);
        }
        if (lower.contains("wrong state")) {
            return new FlowWrongStateException(message, error);
        }
        if (lower.contains("lock") && lower.contains("held")) {
            return new LockHeldException(message, error);
        }
        if (lower.contains("wrong owner") || lower.contains("not held")) {
            return new LockNotOwnedException(message, error);
        }
        if (lower.contains("busy") || lower.contains("overloaded")) {
            return new OverloadedException(message, error);
        }
        if (lower.contains("syntax error") || lower.contains("wrong number of arguments")) {
            return new InvalidCommandException(message, error);
        }
        return new FerricStoreException(message, error);
    }
}
