# FerricStore Java SDK

Java 21+ SDK for FerricStore and FerricFlow.

FerricFlow is an explicit durable state-machine layer over FerricStore. Your application runs normal Java code. FerricFlow stores workflow state, leases, retry data, named values, history, signals, and terminal status.

```text
FLOW.CREATE -> FLOW.CLAIM_DUE -> handler -> FLOW.TRANSITION / COMPLETE / FAIL / RETRY
```

## Modules

- `com.ferricstore:ferricstore-java` - core SDK and RESP command helpers.
- `com.ferricstore:ferricstore-spring-boot-starter` - Spring Boot auto-configuration.
- `com.ferricstore:ferricstore-spring-statemachine` - optional Spring Statemachine adapter for workflow graph validation.
- `ferricstore-examples` - compile-checked example programs.

## Maven

```xml
<dependency>
  <groupId>com.ferricstore</groupId>
  <artifactId>ferricstore-java</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Spring Boot:

```xml
<dependency>
  <groupId>com.ferricstore</groupId>
  <artifactId>ferricstore-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Optional Spring Statemachine adapter:

```xml
<dependency>
  <groupId>com.ferricstore</groupId>
  <artifactId>ferricstore-spring-statemachine</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Local FerricStore

```bash
docker compose up -d ferricstore
```

Default URL:

```text
redis://127.0.0.1:6379/0
```

## Durable Queue

```java
try (FerricStoreClient client = FerricStoreClient.connect("redis://127.0.0.1:6379/0", new JsonCodec())) {
    Queue queue = new QueueClient(client).queue("email");

    queue.enqueue("email-1", Map.of("template", "welcome", "userId", "user-1"));

    QueueWorkerResult result = queue.worker("email-worker-1")
        .batchSize(256)
        .concurrency(128)
        .virtualThreads()
        .runOnce(job -> {
            System.out.println(job.id() + " " + job.payload());
            return Map.of("sent", true);
        });
}
```

The low-level client is synchronous and blocking. Worker concurrency is handled at the worker layer: a worker claims a batch of durable leases, then processes those jobs concurrently before writing complete, retry, fail, or transition commands back to FerricStore. `virtualThreads()` uses Java virtual threads and still respects the configured `concurrency` limit. Spring apps can pass an application-owned `ExecutorService` with `.executor(...)`.

## Explicit Workflow

```java
try (FerricStoreClient client = FerricStoreClient.connect("redis://127.0.0.1:6379/0", new JsonCodec())) {
    Workflow order = new WorkflowClient(client).workflow("order", "created");

    order.state("created", ctx -> {
        chargeCard(ctx.payload());
        return Outcomes.transition("charged");
    });

    order.state("charged", ctx -> {
        sendReceipt(ctx.id());
        return Outcomes.complete(Map.of("ok", true));
    });

    order.start("order-1", Map.of("amount", 42, "userId", "user-1"));
    order.worker("order-worker-1", List.of("created", "charged"))
        .batchSize(128)
        .concurrency(64)
        .virtualThreads()
        .runOnce();
}
```

Handlers return explicit outcomes:

- `Outcomes.transition("next_state")`
- `Outcomes.complete(result)`
- `Outcomes.retry(error)`
- `Outcomes.fail(error)`

FerricFlow does not replay Java handler code. Workers claim durable state, run normal Java code, then write the next state through the FerricFlow API.

## Spring Statemachine Adapter

The optional Spring Statemachine module uses Spring Statemachine for graph validation only. FerricStore remains the only persistence layer for workflow state, leases, retries, history, and terminal status.

```java
FerricFlowStateMachine graph = FerricFlowStateMachine.builder(orderStateMachineFactory).build();

Workflow order = new WorkflowClient(client).workflow("order", "created")
    .state("created", ctx -> graph.apply(ctx, "CHARGE"))
    .state("charged", ctx -> graph.apply(ctx, "COMPLETE", Map.of("ok", true)));

order.worker("order-worker-1", List.of("created", "charged"))
    .concurrency(64)
    .virtualThreads()
    .runOnce();
```

Spring Statemachine actions and guards can still use FerricStore APIs through message headers:

```java
action(context -> {
    FerricStoreClient store = FerricStoreStateMachineContext.client(context);
    WorkflowContext workflow = FerricStoreStateMachineContext.workflowContext(context);
    store.kv().set("order:" + workflow.id(), Map.of("charged", true));
});
```

The adapter restores the Spring machine from `WorkflowContext.state()` on every job and writes the result back through FerricFlow outcomes. Do not configure Spring Statemachine persistence as the source of truth for these workflows.

With Spring Boot, define one `StateMachineFactory<String, String>` bean and include both the starter and statemachine adapter; the starter exposes a `FerricFlowStateMachine` bean for DI.

## Low-Level Flow Commands

```java
client.create(CreateOptions.builder("order-1", "order")
    .state("created")
    .payload(Map.of("amount", 42))
    .idempotent(true)
    .build());

List<FlowRecord> jobs = client.claimDue(ClaimDueOptions.builder("order", "worker-1")
    .state("created")
    .payload(true)
    .leaseMs(30_000)
    .limit(10)
    .build());

for (FlowRecord job : jobs) {
    client.transition(TransitionOptions.builder(job.id(), job.state(), "charged", job.leaseToken(), job.fencingToken())
        .partitionKey(job.partitionKey())
        .build());
}

List<ClaimedItem> compactJobs = client.claimJobs(ClaimDueOptions.builder("order", "worker-1")
    .state("charged")
    .limit(10)
    .build());

for (ClaimedItem job : compactJobs) {
    client.complete(CompleteOptions.builder(job.id(), job.leaseToken(), job.fencingToken())
        .partitionKey(job.partitionKey())
        .build());
}
```

Use `claimDue` when handlers need hydrated workflow records and payloads. Use `claimJobs` when a worker only needs id, partition, lease token, and fencing token for a write such as complete, retry, or fail.

## FerricStore KV And Data Structures

The same client exposes typed helpers for FerricStore's Redis-compatible store commands:

```java
client.kv().set("user:1", Map.of("name", "Ada"), 60_000L, false);
Object user = client.kv().get("user:1");

client.hash().hset("user:1:profile", Map.of("email", "ada@example.com"));
client.lists().lpush("jobs", Map.of("id", "job-1"));
client.sets().sadd("seen-users", "user:1");
client.zset().zadd("leaderboard", List.of(new ZAddMember(42, "user:1")));
client.stream().xadd("events", "*", Map.of("type", "created", "id", "user:1"));
client.json().set("user:1:json", "$", Map.of("name", "Ada"));
client.bloom().add("seen-filter", "user:1");
```

Available helpers: `kv`, `hash`, `lists`, `sets`, `zset`, `stream`, `bitmap`, `hyperloglog`, `geo`, `json`, `bloom`, `cuckoo`, `cms`, `topk`, and `tdigest`.

Use `client.command(...)` for commands that do not have a typed helper yet or for connection-state flows.

## Spring Boot

The starter contributes `Codec`, `FerricStoreClient`, `QueueClient`, and `WorkflowClient` beans when you have not defined your own.

```yaml
ferricstore:
  url: redis://127.0.0.1:6379/0
  codec: json
```

## Examples

Compile-checked examples live under `ferricstore-examples/src/main/java/com/ferricstore/examples`:

- `DurableQueueExample`
- `OrderWorkflowExample`
- `FanoutExample`
- `SignalsExample`
- `ValueRefsExample`
- `StoreUsageExample`

## Development

```bash
mise install
mise exec -- mvn test
```

Run integration tests:

```bash
docker compose up -d ferricstore
scripts/wait-for-ferricstore.sh
FERRICSTORE_INTEGRATION=1 mise exec -- mvn -pl ferricstore-java -am -Dtest=FerricStoreIntegrationTest test
docker compose down -v
```

Generate API docs:

```bash
mise exec -- mvn -B -DskipTests javadoc:aggregate
rsync -a --delete target/reports/apidocs/ docs/api/
```

## Design Notes

The Java SDK borrows useful ergonomics from Temporal Java, Restate Java/Kotlin, DBOS Java/Spring, and the FerricStore Python SDK, but the runtime model is FerricFlow's explicit state machine:

- workflow progress is stored as state transitions, not as a replayed Java stack;
- current state, lease owner, retry data, history, values, and next claimable state are workflow data;
- the same flow can be processed by services in different languages over RESP;
- Spring support is auto-configuration, not instrumentation of business methods.

See [docs/design.md](docs/design.md) and [docs/python-parity.md](docs/python-parity.md).
