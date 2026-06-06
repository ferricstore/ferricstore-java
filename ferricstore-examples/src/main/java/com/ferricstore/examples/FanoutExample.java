package com.ferricstore.examples;

import com.ferricstore.ChildSpec;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.SpawnChildrenOptions;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.util.List;
import java.util.Map;

public final class FanoutExample {
    private FanoutExample() {
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Workflow image = new WorkflowClient(client).workflow("image", "received");

            image.state("received", ctx -> {
                ctx.client().spawnChildren(SpawnChildrenOptions.builder(ctx.id(), List.of(
                        new ChildSpec(ctx.id() + ":small", "resize", Map.of("imageId", ctx.id(), "size", "small")),
                        new ChildSpec(ctx.id() + ":large", "resize", Map.of("imageId", ctx.id(), "size", "large"))))
                    .partitionKey(ctx.partitionKey())
                    .leaseToken(ctx.job().leaseToken())
                    .fencingToken(ctx.job().fencingToken())
                    .build());
                return Outcomes.transition("waiting_for_resizes");
            });

            image.state("waiting_for_resizes", ctx -> Outcomes.complete(Map.of("fanoutStarted", true)));

            image.start("image-" + System.currentTimeMillis(), Map.of("uploadId", "upload-1"));
            System.out.println(image.worker("image-parent-worker-1", List.of("received", "waiting_for_resizes")).runOnce());
        }
    }
}
