package com.ferricstore;

public class FerricStoreException extends RuntimeException {
    public FerricStoreException(String message) {
        super(message);
    }

    public FerricStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
