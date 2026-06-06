package com.ferricstore;

public class FlowNotFoundException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public FlowNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
