package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.util.List;
import java.util.Map;

public final class OrderWorkflowExample {
    private OrderWorkflowExample() {
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Workflow order = new WorkflowClient(client).workflow("order", "created");

            order.state("created", ctx -> {
                System.out.println("charge " + ctx.id() + " " + ctx.payload());
                return Outcomes.transition("charged");
            });

            order.state("charged", ctx -> {
                System.out.println("receipt " + ctx.id());
                return Outcomes.complete(Map.of("ok", true));
            });

            order.start("order-" + System.currentTimeMillis(), Map.of("amount", 42, "userId", "user-1"));
            int applied = order.worker("order-worker-1", List.of("created", "charged")).runOnce();

            System.out.println("applied=" + applied);
        }
    }
}
