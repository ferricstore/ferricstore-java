# FerricStore Java SDK

Java 21+ SDK for FerricStore Flow commands over Redis RESP3.

The SDK keeps payloads as raw `byte[]`. It does not JSON encode user data and it does
not put payloads into client-side indexes. Serialization belongs to the application.

## Toolchain

```bash
mise install
mise exec -- mvn test
```

`mise.toml` pins:

- Java: Temurin 21
- Maven: 3.9.11

## Client

```java
try (FerricStoreClient client = FerricStoreClient.connect("redis://127.0.0.1:6379")) {
    client.create(CreateOptions.builder("flow-1", "order")
        .state("created")
        .partitionKey("tenant-a:device-1")
        .payload(orderBytes)
        .build());

    List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder("order", "worker-1")
        .state("created")
        .partitionKey("tenant-a:device-1")
        .limit(100)
        .leaseMs(30_000)
        .build());

    for (FlowRecord job : jobs) {
        client.complete(CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
            .partitionKey(job.partitionKey())
            .result("ok".getBytes(StandardCharsets.UTF_8))
            .build());
    }
}
```

## Connection Management

`FerricStoreClient.connect(uri)` owns the Jedis connection and closes it when the
client closes.

For production apps that already manage Redis lifecycle, pass an executor:

```java
JedisPooled jedis = new JedisPooled("redis://127.0.0.1:6379");
FerricStoreClient client = FerricStoreClient.fromExecutor(new JedisRedisExecutor(jedis, false));
```

If `closeClient` is `false`, the caller owns closing Jedis.

## Supported Commands

- `FLOW.CREATE`
- `FLOW.CREATE_MANY`
- `FLOW.CLAIM_DUE`
- `FLOW.COMPLETE`
- `FLOW.TRANSITION`
- `FLOW.COMPLETE_MANY`
- `INCR` helper for DBOS-style benchmarks/examples

The command layer is executor-based, so another Redis client can be adapted without
changing workflow code.
