package com.ferricstore;

public class FlowAlreadyExistsException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public FlowAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
