package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class SignalsExample {
    private static final System.Logger LOG = System.getLogger(SignalsExample.class.getName());

    private SignalsExample() {}

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Workflow review =
                    new WorkflowClient(client).workflow("document_review", "pending_review");
            String id = "review-" + System.currentTimeMillis();

            review.state(
                    "approved",
                    ctx ->
                            Outcomes.complete(
                                    Map.of(
                                            "approvedAt", ctx.value("approvedAt"),
                                            "approvedBy", ctx.value("approvedBy"))));

            review.start(id, Map.of("documentId", "doc-1"));
            client.signal(
                    id,
                    "approve",
                    "approved",
                    null,
                    Map.of("approvedAt", Instant.now().toString(), "approvedBy", "user-1"));

            LOG.log(
                    System.Logger.Level.INFO,
                    "{0}",
                    review.worker("review-worker-1", List.of("approved")).runOnce());
        }
    }
}
