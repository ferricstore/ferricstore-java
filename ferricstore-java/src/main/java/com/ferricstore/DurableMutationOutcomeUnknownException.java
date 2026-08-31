package com.ferricstore;

/**
 * A durable mutation may have committed, but its refreshed claim was not safely observed.
 *
 * <p>Do not issue a fallback transition, retry, or failure with the stale claim. Recover by reading
 * or reclaiming the workflow after its lease expires.
 */
public final class DurableMutationOutcomeUnknownException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    DurableMutationOutcomeUnknownException(String operation, Throwable cause) {
        super(
                operation
                        + " outcome is unknown; recover or reclaim the workflow instead of reusing"
                        + " the stale claim",
                cause);
    }
}
