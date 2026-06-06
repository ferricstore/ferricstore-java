package com.ferricstore;

public class LockHeldException extends FerricStoreException {
    public LockHeldException(String message, Throwable cause) {
        super(message, cause);
    }
}
