package com.ferricstore;

public class LockHeldException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public LockHeldException(String message, Throwable cause) {
        super(message, cause);
    }
}
