# FerricStore Java SDK

Java 17+ SDK for FerricStore and FerricFlow. Java 21+ applications may opt into virtual-thread workers.

FerricFlow is an explicit durable state-machine layer over FerricStore. Your application runs normal Java code. FerricFlow stores workflow state, leases, retry data, named values, history, signals, and terminal status.

```text
FLOW.CREATE -> FLOW.CLAIM_DUE -> handler -> FLOW.TRANSITION / COMPLETE / FAIL / RETRY
```

## Modules

- `io.github.ferricstore:ferricstore-java` - framework-neutral core SDK and native-protocol command helpers.
- `io.github.ferricstore:ferricstore-spring-boot-starter` - Spring Boot auto-configuration.
- `io.github.ferricstore:ferricstore-spring-statemachine` - optional Spring Statemachine adapter for workflow graph validation.
- `ferricstore-examples` - compile-checked example programs.

## Maven

Artifacts are published under the FerricStore GitHub organization namespace. Java packages remain under `com.ferricstore`.

```xml
<dependency>
  <groupId>io.github.ferricstore</groupId>
  <artifactId>ferricstore-java</artifactId>
  <version>0.1.2</version>
</dependency>
```

Spring Boot:

```xml
<dependency>
  <groupId>io.github.ferricstore</groupId>
  <artifactId>ferricstore-spring-boot-starter</artifactId>
  <version>0.1.2</version>
</dependency>
```

Optional Spring Statemachine adapter:

```xml
<dependency>
  <groupId>io.github.ferricstore</groupId>
  <artifactId>ferricstore-spring-statemachine</artifactId>
  <version>0.1.2</version>
</dependency>
```

## TCP/TLS And HTTP/HTTPS

The command API is transport-independent. The URL selects the network layer:

```java
FerricStoreClient tcp = FerricStoreClient.connect(
    "ferric://127.0.0.1:6388", new JsonCodec());

HttpTransportOptions http = HttpTransportOptions.builder()
    .username("lambda-user")
    .password(System.getenv("FERRICSTORE_PASSWORD"))
    .maxConcurrentRequests(100)
    .build();
FerricStoreClient https = FerricStoreClient.connect(
    "https://gateway.example", new JsonCodec(), http);
```

HTTP also supports bearer tokens and custom headers. It uses Java 17's persistent `HttpClient`, defaults to HTTP/1.1, and sends an entire SDK pipeline in one HTTP request. Redirects are followed by default and authentication headers are preserved, including across origins, so deployments that allow redirects must trust every redirect target. API gateways should use `307` or `308` when the redirected request must remain a `POST`; normal HTTP semantics may change `301`, `302`, or `303` to `GET`. Set `.redirects(HttpClient.Redirect.NEVER)` when redirects are not acceptable.

For a private native CA, build an `SSLContext` and pass it through `NativeTransportOptions.builder().sslContext(context)`. The equivalent HTTPS option is available on `HttpTransportOptions`.

Most commands work unchanged on both transports. HTTP supports blocking list operations and `XREAD`/`XREADGROUP` as long-lived single requests; the SDK extends the request deadline by each finite blocking timeout and disables its default deadline for an explicit infinite wait. Native TCP/TLS is required for connection-affine transactions, Pub/Sub subscriptions, `FETCH_OR_COMPUTE*`, and session-control commands. HTTP rejects those commands before sending a request; authentication is supplied in HTTP headers rather than with `AUTH`. Publishing ordinary Pub/Sub messages remains available over HTTP.

Run the complete HTTP-compatible integration surface through a real TLS
listener with ACL authentication using:

```bash
FERRICSTORE_IMAGE=quay.io/ferricstore/ferricstore:0.11.11@sha256:d9f488539f0d6c1a513d2315e7a9c2947cc795b393f3774c9de8ba5e5b5c21b5 \
  scripts/run-http-integration.sh
```

The runner creates a private CA, verifies that unauthenticated access and a
restricted user's forbidden `SET` are rejected, and supplies
`FERRICSTORE_USERNAME`, `FERRICSTORE_PASSWORD`, and `FERRICSTORE_CA_FILE`.
It also drives 1,024 operation rounds across 32 concurrent callers through one
shared HTTP client, checks response correlation and atomic updates, and proves
that 16 blocking requests are simultaneously in flight at the server.
Connection-affine tests remain in the native integration job.

## Local FerricStore

```bash
docker compose up -d ferricstore
```

Default URL:

```text
ferric://127.0.0.1:6388
```

## Durable Queue

```java
try (FerricStoreClient client = FerricStoreClient.connect("ferric://127.0.0.1:6388", new JsonCodec())) {
    Queue queue = new QueueClient(client).queue("email");

    queue.enqueue("email-1", Map.of("template", "welcome", "userId", "user-1"));

    QueueWorkerResult result = queue.worker("email-worker-1")
        .batchSize(256)
        .concurrency(128)
        .runOnce(job -> {
            System.out.println(job.id() + " " + job.payload());
            return Map.of("sent", true);
        });
}
```

The low-level client is synchronous and blocking. Worker concurrency is handled at the worker layer: a worker claims a batch of durable leases, then processes those jobs concurrently before writing complete, retry, fail, or transition commands back to FerricStore. Java 17 uses a bounded platform-thread pool. On Java 21+, `.virtualThreads()` selects virtual threads and still respects the configured `concurrency` limit; on Java 17 it fails clearly instead of silently changing behavior. Any application—not only Spring—may pass an application-owned `ExecutorService` with `.executor(...)`; the SDK never closes a supplied executor.

For Lambda or another one-shot invocation, use `runOnce(...)` as above. For a long-running service, keep the execution resources alive across polls with a session while retaining control of scheduling and shutdown:

```java
QueueWorker worker = queue.worker("email-worker-1")
    .batchSize(256)
    .concurrency(128);

try (QueueWorkerSession session = worker.openSession(job -> sendEmail(job))) {
    while (!stopping.get()) {
        QueueWorkerResult result = session.runOnce();
        if (result.claimed() == 0) {
            Thread.sleep(50);
        }
    }
}
```

A session permits one active `runOnce` call, reuses its executor across calls, and drains an active batch for up to 30 seconds on `close()`. Use `close(Duration)` for an explicit bound. Timeout cancellation and Java interruption are best effort; FerricStore lease and fencing tokens remain the authority that rejects stale completion after ownership changes. The SDK does not create a background scheduler, Reactor runtime, or global thread pool.

## Explicit Workflow

```java
try (FerricStoreClient client = FerricStoreClient.connect("ferric://127.0.0.1:6388", new JsonCodec())) {
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

The `json` helper stores and retrieves complete JSON documents through FerricStore's built-in string commands. Its supported path is `$`; FerricStore OSS does not expose RedisJSON path-mutation commands.

Use `client.command(...)` for commands that do not have a typed helper yet or for connection-state flows.

## Spring Boot

The starter contributes `Codec`, `FerricStoreClient`, `QueueClient`, and `WorkflowClient` beans when you have not defined your own.

```yaml
ferricstore:
  url: https://gateway.example
  codec: json
  http:
    username: lambda-user
    password: ${FERRICSTORE_PASSWORD}
    max-concurrent-requests: 100
```

The starter accepts bearer authentication, Basic username/password, custom headers, timeouts, request/response limits, redirect policy, and concurrency under `ferricstore.http`. Plain Java applications use the same `HttpTransportOptions` directly; Spring is optional.

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
mise run test
mise run test:java17
```

Run integration tests:

```bash
docker compose up -d ferricstore
scripts/wait-for-ferricstore.sh
mise run integration:java17
mise run integration:java21
docker compose down -v

# Authenticated TLS HTTP integration using the pinned Docker image
mise run integration:http:java17
mise run integration:http:java21
```

Both Java 17 and Java 21 execute the identical full command and shared-client
concurrency suites. The native integration also includes a blocked-lane test
proving that unrelated lanes continue on the same TCP connection.

Generate API docs:

```bash
mise exec -- mvn -B -DskipTests javadoc:aggregate
rsync -a --delete target/reports/apidocs/ docs/api/
```

## Design Notes

The Java SDK borrows useful ergonomics from Temporal Java, Restate Java/Kotlin, DBOS Java/Spring, and the FerricStore Python SDK, but the runtime model is FerricFlow's explicit state machine:

- workflow progress is stored as state transitions, not as a replayed Java stack;
- current state, lease owner, retry data, history, values, and next claimable state are workflow data;
- the same flow can be processed by services in different languages over native TCP/TLS or stateless HTTP/HTTPS;
- Spring support is auto-configuration, not instrumentation of business methods.

See [docs/design.md](docs/design.md) and [docs/python-parity.md](docs/python-parity.md).
