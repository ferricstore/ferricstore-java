package com.ferricstore;

/**
 * Decodes a journaled durable-step result into the caller's Java type.
 *
 * @param <T> decoded result type
 */
@FunctionalInterface
public interface DurableResultDecoder<T> {
    /**
     * Decodes the exact bytes stored by FerricStore.
     *
     * @param encoded stored result bytes
     * @return the decoded result
     */
    T decode(byte[] encoded);
}
