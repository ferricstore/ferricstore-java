package com.ferricstore;

public class StaleLeaseException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public StaleLeaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
