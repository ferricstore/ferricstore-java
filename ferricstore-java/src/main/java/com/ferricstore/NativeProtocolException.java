package com.ferricstore;

/** Indicates an invalid or incompatible FerricStore native protocol exchange. */
public final class NativeProtocolException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public NativeProtocolException(String message) {
        super(message);
    }

    public NativeProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
