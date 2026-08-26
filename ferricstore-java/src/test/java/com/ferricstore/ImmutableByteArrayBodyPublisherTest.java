package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class ImmutableByteArrayBodyPublisherTest {
    @Test
    void publishesTheExactSliceAsAnIndependentReadOnlyViewForEverySubscription() {
        byte[] body = {1, 2, 3, 4};
        ImmutableByteArrayBodyPublisher publisher = new ImmutableByteArrayBodyPublisher(body, 3);

        RecordingSubscriber first = new RecordingSubscriber();
        RecordingSubscriber second = new RecordingSubscriber();
        publisher.subscribe(first);
        publisher.subscribe(second);

        assertEquals(3, publisher.contentLength());
        assertArrayEquals(new byte[] {1, 2, 3}, first.body());
        assertArrayEquals(new byte[] {1, 2, 3}, second.body());
        assertTrue(first.buffers.get(0).isReadOnly());
        assertTrue(second.buffers.get(0).isReadOnly());
        assertNotSame(first.buffers.get(0), second.buffers.get(0));
    }

    @Test
    void rejectsNonPositiveDemandAccordingToTheFlowContract() {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ImmutableByteArrayBodyPublisher publisher =
                new ImmutableByteArrayBodyPublisher(new byte[] {1}, 1);

        publisher.subscribe(new InvalidDemandSubscriber(failure));

        assertInstanceOf(IllegalArgumentException.class, failure.get());
    }

    private static final class InvalidDemandSubscriber implements Flow.Subscriber<ByteBuffer> {
        private final AtomicReference<Throwable> failure;

        private InvalidDemandSubscriber(AtomicReference<Throwable> failure) {
            this.failure = failure;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(0);
        }

        @Override
        public void onNext(ByteBuffer item) {}

        @Override
        public void onError(Throwable error) {
            failure.set(error);
        }

        @Override
        public void onComplete() {}
    }

    private static final class RecordingSubscriber implements Flow.Subscriber<ByteBuffer> {
        private final List<ByteBuffer> buffers = new ArrayList<>();

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(1);
        }

        @Override
        public void onNext(ByteBuffer item) {
            buffers.add(item);
        }

        @Override
        public void onError(Throwable error) {
            throw new AssertionError(error);
        }

        @Override
        public void onComplete() {}

        private byte[] body() {
            ByteBuffer copy = buffers.get(0).duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
    }
}
