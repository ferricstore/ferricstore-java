package com.ferricstore;

public class InvalidCommandException extends FerricStoreException {
    public InvalidCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
