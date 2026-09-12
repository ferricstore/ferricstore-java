package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.util.List;
import java.util.Map;

public final class OrderWorkflowExample {
    private static final System.Logger LOG = System.getLogger(OrderWorkflowExample.class.getName());

    private OrderWorkflowExample() {}

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Workflow order = new WorkflowClient(client).workflow("order", "created");

            order.state(
                    "created",
                    ctx -> {
                        ChargeReceipt receipt =
                                ctx.step(
                                        "charge-customer:v1",
                                        () ->
                                                chargeCustomer(
                                                        ctx.payload(),
                                                        ctx.id() + ":charge-customer:v1"),
                                        "charged",
                                        ChargeReceipt.class);
                        LOG.log(System.Logger.Level.INFO, "charged {0}", receipt.providerId());
                        return Outcomes.transition("charged");
                    });

            order.state(
                    "charged",
                    ctx -> {
                        ctx.step(
                                "send-receipt:v1",
                                () -> {
                                    sendReceipt(ctx.id(), ctx.id() + ":send-receipt:v1");
                                    return null;
                                },
                                "done",
                                Void.class);
                        return Outcomes.complete(Map.of("ok", true));
                    });

            order.start(
                    "order-" + System.currentTimeMillis(),
                    Map.of("amount", 42, "userId", "user-1"));
            int applied = order.worker("order-worker-1", List.of("created", "charged")).runOnce();

            LOG.log(System.Logger.Level.INFO, "applied={0}", applied);
        }
    }

    private static ChargeReceipt chargeCustomer(Object order, String idempotencyKey) {
        // A real provider call must receive the same idempotency key on every retry.
        return new ChargeReceipt("provider:" + idempotencyKey, order);
    }

    private static void sendReceipt(String orderId, String idempotencyKey) {
        // The provider must deduplicate this stable key if the worker stops before the commit.
        LOG.log(
                System.Logger.Level.INFO,
                "receipt order={0} idempotency-key={1}",
                orderId,
                idempotencyKey);
    }

    /**
     * Example provider result that can be deserialized again during durable-step replay.
     *
     * @param providerId the external provider's stable result identity
     * @param order the order payload returned with the provider result
     */
    public record ChargeReceipt(String providerId, Object order) {}
}
