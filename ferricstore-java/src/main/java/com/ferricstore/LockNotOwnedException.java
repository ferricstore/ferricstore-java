package com.ferricstore;

public class LockNotOwnedException extends FerricStoreException {
    public LockNotOwnedException(String message, Throwable cause) {
        super(message, cause);
    }
}
