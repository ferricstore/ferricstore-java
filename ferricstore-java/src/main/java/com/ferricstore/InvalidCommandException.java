package com.ferricstore;

public class InvalidCommandException extends FerricStoreException
        implements RequestDeliveryFailure {
    private static final long serialVersionUID = 1L;
    private final RequestDelivery delivery;

    public InvalidCommandException(String message) {
        super(message);
        delivery = RequestDelivery.NOT_SENT;
    }

    public InvalidCommandException(String message, Throwable cause) {
        super(message, cause);
        delivery = RequestDelivery.REJECTED;
    }

    @Override
    public RequestDelivery delivery() {
        return delivery;
    }
}
