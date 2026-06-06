package com.ferricstore;

public class LockNotOwnedException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public LockNotOwnedException(String message, Throwable cause) {
        super(message, cause);
    }
}
