package com.ferricstore;

public class FerricStoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public FerricStoreException(String message) {
        super(message);
    }

    public FerricStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
