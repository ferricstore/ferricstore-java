package com.ferricstore;

import javax.net.ssl.SSLContext;

/** Configuration for the native TCP/TLS transport. */
public final class NativeTransportOptions {
    private final SSLContext sslContext;

    private NativeTransportOptions(Builder builder) {
        sslContext = builder.sslContext;
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

    public static final class Builder {
        private SSLContext sslContext;

        private Builder() {}

        /**
         * Supplies trust/key material for {@code ferrics://}; system defaults are used otherwise.
         */
        public Builder sslContext(SSLContext value) {
            sslContext = value;
            return this;
        }

        public NativeTransportOptions build() {
            return new NativeTransportOptions(this);
        }
    }
}
