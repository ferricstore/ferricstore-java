package com.ferricstore;

import static com.ferricstore.IntegrationTestEnvironment.assumeIntegration;
import static com.ferricstore.IntegrationTestEnvironment.connectJson;
import static com.ferricstore.IntegrationTestEnvironment.suffix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FerricStoreRewindReasonIntegrationTest {
    @Test
    void persistsRewindReasonAcrossRecordValueAndHistory() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:rewind-reason:" + testId;
            String id = "java-sdk:rewind-reason:flow:" + testId;
            String partition = id + ":partition";
            long now = System.currentTimeMillis();

            client.create(
                    CreateOptions.builder(id, type)
                            .state("queued")
                            .partitionKey(partition)
                            .payload(Map.of("name", "rewind-reason"))
                            .nowMs(now)
                            .runAtMs(now)
                            .idempotent(true)
                            .build());
            List<ClaimedItem> jobs =
                    client.claimJobs(
                            ClaimDueOptions.builder(type, "java-sdk-rewind-reason-worker")
                                    .state("queued")
                                    .partitionKey(partition)
                                    .limit(1)
                                    .nowMs(now)
                                    .leaseMs(30_000)
                                    .build());
            assertEquals(1, jobs.size());
            ClaimedItem job = jobs.get(0);
            String createdEventId = eventId(client.history(id, partition, 10).get(0));
            FlowRecord completed =
                    (FlowRecord)
                            client.complete(
                                    CompleteOptions.builder(
                                                    job.id(), job.leaseToken(), job.fencingToken())
                                            .partitionKey(partition)
                                            .returnRecord(true)
                                            .build());
            assertNotNull(completed);
            assertNull(completed.leaseToken());

            String reason = "java-sdk-rewind-reason:" + testId;
            FlowRecord rewound =
                    (FlowRecord)
                            client.rewind(
                                    id,
                                    createdEventId,
                                    partition,
                                    "completed",
                                    now,
                                    reason,
                                    null,
                                    true);
            assertEquals("queued", rewound.state());
            assertEquals(completed.fencingToken() + 1, rewound.fencingToken());
            assertEquals(completed.version() + 1, rewound.version());
            assertNull(rewound.leaseToken());

            Object reasonRef = rewound.raw().get("error_ref");
            assertNotNull(reasonRef);
            assertEquals(List.of(reason), client.valueMGet(List.of(text(reasonRef))));
            assertTrue(
                    client.history(id, partition, 20).stream()
                            .anyMatch(event -> hasReasonEvent(event, reasonRef)));
        }
    }

    private static String eventId(Object event) {
        if (event instanceof List<?> list && !list.isEmpty()) return text(list.get(0));
        Object value = field(event, "event_id");
        if (value == null) value = field(event, "id");
        assertNotNull(value);
        return text(value);
    }

    private static Object eventField(Object event, String name) {
        Object fields = event;
        if (event instanceof List<?> list && list.size() > 1) fields = list.get(1);
        Object value = field(fields, name);
        if (value == null && fields instanceof Map<?, ?> map && map.get("fields") != null) {
            value = field(map.get("fields"), name);
        }
        return value;
    }

    private static boolean hasReasonEvent(Object event, Object reasonRef) {
        String eventName = text(eventField(event, "event"));
        String eventReasonRef = text(eventField(event, "error_ref"));
        return "rewound".equals(eventName) && text(reasonRef).equals(eventReasonRef);
    }

    private static Object field(Object source, String name) {
        if (source instanceof Map<?, ?> map) {
            Object value = map.get(name);
            return value == null ? map.get(name.getBytes(StandardCharsets.UTF_8)) : value;
        }
        return null;
    }

    private static String text(Object value) {
        if (value instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        return Resp.string(value);
    }
}
