package com.ferricstore.examples;

import com.ferricstore.CreateOptions;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.Outcomes;
import com.ferricstore.Workflow;
import com.ferricstore.WorkflowClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class ValueRefsExample {
    private ValueRefsExample() {
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            Object rawRef = client.valuePut(Map.of("plan", "enterprise", "userId", "user-1"), "profile", null, null, 3_600_000L);
            String profileRef = rawRef instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(rawRef);

            Workflow account = new WorkflowClient(client).workflow("account", "hydrate");
            account.state("hydrate", ctx -> Outcomes.complete(Map.of("profile", ctx.value("profile"))));

            String id = "account-" + System.currentTimeMillis();
            client.create(CreateOptions.builder(id, "account")
                .state("hydrate")
                .idempotent(true)
                .valueRef("profile", profileRef)
                .build());

            System.out.println(account.worker("account-worker-1", List.of("hydrate")).runOnce());
        }
    }
}
