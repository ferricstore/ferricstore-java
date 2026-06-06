package com.ferricstore;

public class StaleLeaseException extends FerricStoreException {
    public StaleLeaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
