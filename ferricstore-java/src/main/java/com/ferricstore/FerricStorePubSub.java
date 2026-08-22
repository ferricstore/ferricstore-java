package com.ferricstore;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Native pushed Pub/Sub session backed by one owned TCP/TLS connection. */
public final class FerricStorePubSub implements AutoCloseable {
    private final SessionCommandExecutor executor;
    private final Codec codec;
    private final ArrayDeque<PubSubMessage> pending = new ArrayDeque<>();
    private boolean closed;

    FerricStorePubSub(SessionExecutorFactory factory, Codec codec) {
        this.executor = Objects.requireNonNull(factory.openSession(), "session executor");
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    public Object subscribe(String... channels) {
        return subscription("SUBSCRIBE", true, channels);
    }

    public Object unsubscribe(String... channels) {
        return subscription("UNSUBSCRIBE", false, channels);
    }

    public Object psubscribe(String... patterns) {
        return subscription("PSUBSCRIBE", true, patterns);
    }

    public Object punsubscribe(String... patterns) {
        return subscription("PUNSUBSCRIBE", false, patterns);
    }

    /** Waits for one pushed message, returning {@code null} when the timeout expires. */
    public PubSubMessage getMessage(Duration timeout) {
        requireOpen();
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }
        PubSubMessage buffered = pending.pollFirst();
        if (buffered != null) {
            return buffered;
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        Duration remaining = timeout;
        while (!remaining.isNegative()) {
            Object event = executor.pollEvent(remaining);
            if (event == null) {
                return null;
            }
            decodeEvent(event);
            PubSubMessage message = pending.pollFirst();
            if (message != null) {
                return message;
            }
            long nanos = deadline - System.nanoTime();
            if (nanos <= 0) {
                return null;
            }
            remaining = Duration.ofNanos(nanos);
        }
        return null;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        try {
            executor.execute(List.of("UNSUBSCRIBE"));
        } catch (RuntimeException error) {
            failure = error;
        }
        try {
            executor.execute(List.of("PUNSUBSCRIBE"));
        } catch (RuntimeException error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        try {
            executor.close();
        } catch (RuntimeException error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private Object subscription(String command, boolean requireValues, String... values) {
        requireOpen();
        if (requireValues && values.length == 0) {
            throw new IllegalArgumentException(command + " requires at least one value");
        }
        List<Object> args = new ArrayList<>(values.length + 1);
        args.add(command);
        for (String value : values) {
            FlowValidation.requireText(value, command + " value");
            args.add(value);
        }
        return executor.execute(args);
    }

    private void decodeEvent(Object value) {
        Map<String, Object> event = Resp.map(value);
        if (!"PUBSUB_MESSAGE".equals(Resp.string(event.get("event")))) {
            return;
        }
        Map<String, Object> payload = Resp.map(event.get("payload"));
        String kind = Resp.string(payload.get("kind"));
        String channel = Resp.string(payload.get("channel"));
        String pattern = Resp.optionalString(payload.get("pattern"));
        if ("message_batch".equals(kind)) {
            for (Object message : Resp.list(payload.get("messages"))) {
                pending.addLast(
                        new PubSubMessage(
                                "message", channel, decodeMessage(message), pattern, event));
            }
            return;
        }
        if ("message".equals(kind) || "pmessage".equals(kind)) {
            pending.addLast(
                    new PubSubMessage(
                            kind, channel, decodeMessage(payload.get("message")), pattern, event));
        }
    }

    private Object decodeMessage(Object value) {
        return value instanceof byte[] bytes ? codec.decode(bytes) : value;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Pub/Sub session is closed");
        }
    }
}
