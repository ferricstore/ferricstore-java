package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ConnectionSessionsTest {
    @Test
    void transactionUsesOneOwnedConnectionAndDiscardsWhenNotExecuted() {
        SessionFactory factory = new SessionFactory();
        FerricStoreClient client = FerricStoreClient.fromExecutor(factory);

        try (FerricStoreTransaction transaction = client.transaction(List.of("{tenant}:key"))) {
            transaction.command("SET", "{tenant}:key", bytes("value"));
            assertEquals(List.of("OK", "OK"), transaction.execute());
        }

        assertEquals(List.of("WATCH", "{tenant}:key"), factory.sessions.get(0).calls.get(0));
        assertEquals(List.of("MULTI"), factory.sessions.get(0).calls.get(1));
        assertEquals("SET", factory.sessions.get(0).calls.get(2).get(0));
        assertEquals("{tenant}:key", factory.sessions.get(0).calls.get(2).get(1));
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                bytes("value"), (byte[]) factory.sessions.get(0).calls.get(2).get(2));
        assertEquals(List.of("EXEC"), factory.sessions.get(0).calls.get(3));
        assertTrue(factory.sessions.get(0).closed);

        try (FerricStoreTransaction transaction = client.transaction()) {
            assertNotNull(transaction.client());
            // close must discard the active transaction.
        }
        assertEquals(List.of("DISCARD"), factory.sessions.get(1).calls.get(1));
        assertTrue(factory.sessions.get(1).closed);
    }

    @Test
    void connectionSessionsAreUnavailableOnStatelessOrCustomExecutors() {
        FerricStoreClient client = FerricStoreClient.fromExecutor(args -> "OK");

        IllegalStateException transaction =
                assertThrows(IllegalStateException.class, client::transaction);
        assertTrue(transaction.getMessage().contains("native TCP"));
        IllegalStateException pubsub =
                assertThrows(IllegalStateException.class, client::pubsubSession);
        assertTrue(pubsub.getMessage().contains("native TCP"));
    }

    @Test
    void pubsubUsesDedicatedConnectionAndDecodesPushedEvents() {
        SessionFactory factory = new SessionFactory();
        factory.nextEvent =
                java.util.Map.of(
                        "event",
                        "PUBSUB_MESSAGE",
                        "payload",
                        java.util.Map.of(
                                "kind", "message",
                                "channel", bytes("events"),
                                "message", bytes("payload")));
        FerricStoreClient client = FerricStoreClient.fromExecutor(factory, new StringCodec());

        try (FerricStorePubSub pubsub = client.pubsubSession()) {
            pubsub.subscribe("events");
            PubSubMessage message = pubsub.getMessage(Duration.ofMillis(10));
            assertEquals("message", message.kind());
            assertEquals("events", message.channel());
            assertEquals("payload", message.message());
        }

        assertEquals(List.of("SUBSCRIBE", "events"), factory.sessions.get(0).calls.get(0));
        assertTrue(factory.sessions.get(0).closed);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static final class SessionFactory implements CommandExecutor, SessionExecutorFactory {
        private final List<FakeSession> sessions = new ArrayList<>();
        private Object nextEvent;

        @Override
        public Object execute(List<Object> args) {
            return "OK";
        }

        @Override
        public SessionCommandExecutor openSession() {
            FakeSession session = new FakeSession(nextEvent);
            sessions.add(session);
            nextEvent = null;
            return session;
        }
    }

    private static final class FakeSession implements SessionCommandExecutor {
        private final List<List<Object>> calls = new ArrayList<>();
        private Object event;
        private boolean closed;

        private FakeSession(Object event) {
            this.event = event;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(new ArrayList<>(args));
            if ("EXEC".equals(args.get(0))) {
                return List.of("OK", "OK");
            }
            return List.of("WATCH", "MULTI", "DISCARD", "UNSUBSCRIBE", "PUNSUBSCRIBE")
                            .contains(args.get(0))
                    ? "OK"
                    : "QUEUED";
        }

        @Override
        public Object pollEvent(Duration timeout) {
            Object value = event;
            event = null;
            return value;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
