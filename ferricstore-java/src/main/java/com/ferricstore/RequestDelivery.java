package com.ferricstore;

/** Whether a failed request was sent far enough that its mutation may have committed. */
public enum RequestDelivery {
    NOT_SENT,
    REJECTED,
    UNKNOWN
}
