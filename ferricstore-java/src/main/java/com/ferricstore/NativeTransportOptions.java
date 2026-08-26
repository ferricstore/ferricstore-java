package com.ferricstore;

import javax.net.ssl.SSLContext;

/** Configuration for the native TCP/TLS transport. */
public final class NativeTransportOptions {
    static final int DEFAULT_MAX_PENDING_REQUESTS = 1_024;

    private final SSLContext sslContext;
    private final int maxPendingRequests;

    private NativeTransportOptions(Builder builder) {
        sslContext = builder.sslContext;
        maxPendingRequests = builder.maxPendingRequests;
    }

    public static NativeTransportOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    SSLContext sslContext() {
        return sslContext;
    }

    int maxPendingRequests() {
        return maxPendingRequests;
    }

    public static final class Builder {
        private SSLContext sslContext;
        private int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;

        private Builder() {}

        /**
         * Supplies trust/key material for {@code ferrics://}; system defaults are used otherwise.
         */
        public Builder sslContext(SSLContext value) {
            sslContext = value;
            return this;
        }

        /** Limits requests awaiting a response on the multiplexed native connection. */
        public Builder maxPendingRequests(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("maxPendingRequests must be positive");
            }
            maxPendingRequests = value;
            return this;
        }

        public NativeTransportOptions build() {
            return new NativeTransportOptions(this);
        }
    }
}
