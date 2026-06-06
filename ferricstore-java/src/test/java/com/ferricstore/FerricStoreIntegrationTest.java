package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FerricStoreIntegrationTest {
    @Test
    void kvAndFlowRoundTripAgainstLocalServer() {
        assumeTrue(
                "1".equals(System.getenv("FERRICSTORE_INTEGRATION")),
                "set FERRICSTORE_INTEGRATION=1 to run local FerricStore integration tests");

        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            String suffix = Long.toString(System.currentTimeMillis(), 36);

            assertTrue(client.kv().set("it:kv:" + suffix, Map.of("ok", true)));
            assertEquals(Map.of("ok", true), client.kv().get("it:kv:" + suffix));

            String id = "it-flow-" + suffix;
            String partition = "it-partition-" + suffix;
            client.create(
                    CreateOptions.builder(id, "it_order")
                            .state("created")
                            .partitionKey(partition)
                            .payload(Map.of("amount", 42))
                            .idempotent(true)
                            .build());

            List<ClaimedItem> jobs =
                    client.claimJobs(
                            ClaimDueOptions.builder("it_order", "it-worker")
                                    .state("created")
                                    .partitionKey(partition)
                                    .limit(1)
                                    .leaseMs(30_000)
                                    .build());

            assertFalse(jobs.isEmpty());
            ClaimedItem job = jobs.getFirst();
            assertEquals(partition, job.partitionKey());
            client.complete(
                    CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
                            .partitionKey(partition)
                            .result(Map.of("ok", true))
                            .ttlMs(60_000)
                            .build());

            FlowRecord completed = client.get(id, partition);
            assertNotNull(completed);
            assertEquals("completed", completed.state());
        }
    }
}
