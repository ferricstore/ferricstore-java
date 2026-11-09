package com.ferricstore.examples;

import com.ferricstore.ChildGroupResults;
import com.ferricstore.ChildResult;
import com.ferricstore.ChildSpec;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.FlowRecord;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.WaitForChildrenOutcome;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import com.ferricstore.WorkflowWorker;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fans out one resize per image size, waits for all of them, then reads their results.
 *
 * <p>The image Flow waits in {@code waiting_for_resizes} without holding a lease or a worker.
 * FerricStore moves it to {@code resized} once every resize completes, or to {@code resize_failed}
 * at the first failed resize.
 */
public final class FanoutExample {
    private static final System.Logger LOG = System.getLogger(FanoutExample.class.getName());
    private static final Set<String> TERMINAL = Set.of("completed", "failed", "cancelled");

    private FanoutExample() {}

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            FlowRecord image = run(client, "image-" + System.currentTimeMillis());
            ChildGroupResults resizes = client.childResults(image.id(), null, "resizes");
            LOG.log(
                    System.Logger.Level.INFO,
                    "image {0} {1} after {2} resizes",
                    image.id(),
                    image.state(),
                    resizes.summary().completed());
        }
    }

    /** Runs one image through the workflow and returns its terminal record. */
    public static FlowRecord run(FerricStoreClient client, String imageId) {
        WorkflowClient workflows = new WorkflowClient(client);
        Workflow image = workflows.workflow("image", "received");
        Workflow resize = workflows.workflow("resize", "queued");

        // Creating the resizes and parking the image is one fenced command.
        image.state(
                "received",
                ctx ->
                        WaitForChildrenOutcome.builder(
                                        List.of(
                                                new ChildSpec(
                                                        ctx.id() + ":small",
                                                        "resize",
                                                        Map.of(
                                                                "imageId", ctx.id(), "size",
                                                                "small")),
                                                new ChildSpec(
                                                        ctx.id() + ":large",
                                                        "resize",
                                                        Map.of(
                                                                "imageId", ctx.id(), "size",
                                                                "large"))),
                                        "waiting_for_resizes",
                                        "resized",
                                        "resize_failed")
                                .groupId("resizes")
                                .build());

        // Each resize is an ordinary Flow with its own lease, retries, and history.
        resize.state(
                "queued",
                ctx -> {
                    Map<?, ?> payload = (Map<?, ?>) ctx.payload();
                    return Outcomes.complete(
                            Map.of(
                                    "size",
                                    payload.get("size"),
                                    "url",
                                    "s3://images/"
                                            + payload.get("imageId")
                                            + "/"
                                            + payload.get("size")
                                            + ".jpg"));
                });

        image.state(
                "resized",
                ctx -> {
                    List<?> urls =
                            ctx.childResults("resizes").children().stream()
                                    .map(child -> ((Map<?, ?>) child.result()).get("url"))
                                    .toList();
                    return Outcomes.complete(Map.of("urls", urls));
                });

        image.state(
                "resize_failed",
                ctx -> {
                    List<String> failed =
                            ctx.childResults("resizes").children().stream()
                                    .filter(child -> child.status().equals("failed"))
                                    .map(ChildResult::id)
                                    .toList();
                    return Outcomes.fail(Map.of("failed", failed));
                });

        image.start(imageId, Map.of("uploadId", "upload-1"));

        // Production workers are long-running loops, usually in separate services.
        // This example drives both in one process until the image is terminal.
        WorkflowWorker imageWorker =
                image.worker("image-worker-1", List.of("received", "resized", "resize_failed"));
        WorkflowWorker resizeWorker = resize.worker("resize-worker-1", List.of("queued"));
        for (int round = 0; round < 100; round++) {
            FlowRecord record = client.get(imageId, null);
            if (record != null && TERMINAL.contains(record.state())) {
                return record;
            }
            imageWorker.runOnce();
            resizeWorker.runOnce();
        }
        throw new IllegalStateException("image " + imageId + " did not finish");
    }
}
