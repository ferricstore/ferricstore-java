package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.ZAddMember;
import java.util.List;
import java.util.Map;

public final class StoreUsageExample {
    private static final System.Logger LOG = System.getLogger(StoreUsageExample.class.getName());

    private StoreUsageExample() {}

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("FERRICSTORE_URL", "redis://127.0.0.1:6379/0");
        try (FerricStoreClient client = FerricStoreClient.connect(url, new JsonCodec())) {
            String suffix = Long.toString(System.currentTimeMillis(), 36);

            client.kv().set("session:" + suffix, Map.of("userId", "user-1"), 60_000L, false);
            client.hash().hset("user:" + suffix, Map.of("email", "ada@example.com", "plan", "pro"));
            client.lists().rpush("jobs:" + suffix, Map.of("id", "job-1"), Map.of("id", "job-2"));
            client.sets().sadd("seen:" + suffix, "user-1", "user-2");
            client.zset().zadd("scores:" + suffix, List.of(new ZAddMember(42, "user-1")));
            client.stream().xadd("events:" + suffix, "*", Map.of("id", "evt-1", "type", "created"));
            client.json()
                    .set(
                            "json:user:" + suffix,
                            "$",
                            Map.of("id", "user-1", "flags", List.of("beta")));
            client.bloom().add("bf:seen:" + suffix, "user-1");
            client.tdigest().create("latency:" + suffix);
            client.tdigest().add("latency:" + suffix, 12, 18, 31);

            LOG.log(
                    System.Logger.Level.INFO,
                    "{0}",
                    Map.of(
                            "json", client.json().get("json:user:" + suffix, Map.class),
                            "session", client.kv().get("session:" + suffix),
                            "tdigestP95", client.tdigest().quantile("latency:" + suffix, 0.95)));
        }
    }
}
