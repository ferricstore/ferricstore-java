package com.ferricstore.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.ferricstore.ChildGroupResults;
import com.ferricstore.ChildResult;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.FlowRecord;
import com.ferricstore.JsonCodec;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Runs the documented fan-out example against a live server (fan-in needs 0.11.19+). */
final class FanoutExampleIntegrationTest {
    @Test
    void imageWaitsForBothResizesAndReadsTheirResults() {
        assumeTrue(
                "1".equals(System.getenv("FERRICSTORE_INTEGRATION")),
                "set FERRICSTORE_INTEGRATION=1 to run local FerricStore integration tests");
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            String imageId = "java-example:image:" + UUID.randomUUID();

            FlowRecord image = FanoutExample.run(client, imageId);

            assertEquals("completed", image.state());
            ChildGroupResults resizes = client.childResults(imageId, null, "resizes");
            assertEquals("success", resizes.resolved());
            assertEquals(2, resizes.summary().completed());
            assertEquals(
                    List.of(imageId + ":large", imageId + ":small"),
                    resizes.children().stream().map(ChildResult::id).toList());
            assertEquals(
                    "s3://images/" + imageId + "/small.jpg",
                    ((Map<?, ?>) resizes.children().get(1).result()).get("url"));
        }
    }
}
