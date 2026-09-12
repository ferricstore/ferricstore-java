package com.ferricstore;

/** Implemented by transport failures that can state whether a request may have committed. */
@FunctionalInterface
public interface RequestDeliveryFailure {
    RequestDelivery delivery();
}
