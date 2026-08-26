package com.ferricstore;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Flow;

/** One-item HTTP body publisher over a byte array that the SDK owns and never mutates. */
final class ImmutableByteArrayBodyPublisher implements HttpRequest.BodyPublisher {
    private final byte[] body;
    private final int length;

    ImmutableByteArrayBodyPublisher(byte[] body, int length) {
        this.body = Objects.requireNonNull(body, "HTTP request body");
        if (length < 0 || length > body.length) {
            throw new IllegalArgumentException("HTTP request body length is out of bounds");
        }
        this.length = length;
    }

    @Override
    public long contentLength() {
        return length;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber, "HTTP body subscriber")
                .onSubscribe(new BodySubscription(subscriber));
    }

    private final class BodySubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super ByteBuffer> subscriber;
        private boolean terminated;

        private BodySubscription(Flow.Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long demand) {
            synchronized (this) {
                if (terminated) {
                    return;
                }
                terminated = true;
            }
            if (demand <= 0) {
                subscriber.onError(
                        new IllegalArgumentException("HTTP body demand must be positive"));
                return;
            }
            subscriber.onNext(ByteBuffer.wrap(body, 0, length).asReadOnlyBuffer());
            subscriber.onComplete();
        }

        @Override
        public void cancel() {
            synchronized (this) {
                terminated = true;
            }
        }
    }
}
