package com.ferricstore;

public class InvalidCommandException extends FerricStoreException {
    private static final long serialVersionUID = 1L;

    public InvalidCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
