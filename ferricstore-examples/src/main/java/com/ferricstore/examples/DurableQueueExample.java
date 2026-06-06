package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Queue;
import com.ferricstore.QueueClient;
import com.ferricstore.QueueWorkerResult;
import java.util.Map;

public final class DurableQueueExample {
    private static final System.Logger LOG = System.getLogger(DurableQueueExample.class.getName());

    private DurableQueueExample() {}

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Queue queue = new QueueClient(client).queue("thumbnail");
            String id = "thumbnail-" + System.currentTimeMillis();

            queue.enqueue(id, Map.of("imageId", "img-1", "size", "small"));

            QueueWorkerResult result =
                    queue.worker("thumbnail-worker-1")
                            .runOnce(job -> Map.of("generated", true, "jobId", job.id()));

            LOG.log(System.Logger.Level.INFO, "{0}", result);
        }
    }
}
