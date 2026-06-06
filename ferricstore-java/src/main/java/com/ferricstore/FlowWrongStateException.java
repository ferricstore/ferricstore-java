package com.ferricstore;

public class FlowWrongStateException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public FlowWrongStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
