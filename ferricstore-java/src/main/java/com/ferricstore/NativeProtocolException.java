package com.ferricstore;

/** Indicates an invalid or incompatible FerricStore native protocol exchange. */
public final class NativeProtocolException extends FerricStoreException
        implements RequestDeliveryFailure {
    private static final long serialVersionUID = 1L;
    private final RequestDelivery delivery;

    public NativeProtocolException(String message) {
        this(message, null, RequestDelivery.UNKNOWN);
    }

    public NativeProtocolException(String message, Throwable cause) {
        this(message, cause, RequestDelivery.UNKNOWN);
    }

    private NativeProtocolException(String message, Throwable cause, RequestDelivery delivery) {
        super(message, cause);
        this.delivery = delivery;
    }

    static NativeProtocolException notSent(String message) {
        return new NativeProtocolException(message, null, RequestDelivery.NOT_SENT);
    }

    static NativeProtocolException notSent(String message, Throwable cause) {
        return new NativeProtocolException(message, cause, RequestDelivery.NOT_SENT);
    }

    NativeProtocolException asNotSent() {
        return new NativeProtocolException(getMessage(), getCause(), RequestDelivery.NOT_SENT);
    }

    @Override
    public RequestDelivery delivery() {
        return delivery;
    }
}
