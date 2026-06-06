package com.ferricstore;

public class OverloadedException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public OverloadedException(String message, Throwable cause) {
        super(message, cause);
    }
}
