# SDK Design

FerricFlow is centered on durable state-machine records.

The Java SDK keeps that visible. A flow has a `type`, `id`, current `state`, payload/value refs, lease/fencing data, retry metadata, history, and terminal status. Workers claim a state, execute normal Java code, then explicitly write one of four outcomes:

```java
return Outcomes.transition("charged");
return Outcomes.complete(Map.of("ok", true));
return Outcomes.retry(Map.of("reason", "rate limited"));
return Outcomes.fail(Map.of("reason", "bad input"));
```

## What The SDK Does

- Builds typed `FLOW.*` commands over Redis RESP.
- Provides `FerricStoreClient` for direct command control.
- Provides `QueueClient` for durable queue-shaped workloads.
- Provides `WorkflowClient` for explicit state-machine workflows.
- Provides store helpers for KV, hashes, lists, sets, sorted sets, streams, JSON, and probabilistic structures.
- Provides a Spring Boot starter with conditional beans for `Codec`, `FerricStoreClient`, `QueueClient`, and `WorkflowClient`.
- Provides an optional Spring Statemachine adapter for validating state graphs while keeping FerricStore as the only workflow persistence layer.

## What The SDK Does Not Do

- It does not replay workflow code.
- It does not proxy or instrument user methods.
- It does not require annotations for durability.
- It does not require one Java service to own every workflow state.

This matters because a flow can move between services. One Java worker can handle `created`, a Go service can handle `charged`, and a Python service can handle `receipt`. FerricFlow stores the durable state between them.

## Relation To Temporal, Restate, And DBOS

Temporal Java organizes applications around Temporal primitives such as Workflows, Activities, Workers, and Clients: https://docs.temporal.io/develop/java

Restate exposes Services, Virtual Objects, Workflows, state, scheduling, and journaling APIs in its Java/Kotlin SDK: https://docs.restate.dev/category/javakotlin-sdk/

DBOS Java uses workflow/step annotations and proxy registration, with Spring Boot support handling proxy creation and workflow registration: https://docs.dbos.dev/java/reference/workflows-steps

Those projects are useful references for Java ergonomics. FerricFlow's runtime model is different. The important unit is not a replayed workflow function or proxied method. The important unit is the durable flow record and its explicit state transitions.

## Spring Boot Shape

The starter follows Spring Boot's auto-configuration model: it uses `@AutoConfiguration`, `@ConditionalOnMissingBean`, and `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Spring's own guide recommends this imports file for external starter jars and explains that auto-configured beans should back away when users define their own beans: https://docs.spring.io/spring-boot/4.0-SNAPSHOT/reference/features/developing-auto-configuration.html

## Spring Statemachine Adapter

`ferricstore-spring-statemachine` is optional. It lets Spring applications define states, events, guards, and actions with Spring Statemachine, then use `FerricFlowStateMachine` inside a normal FerricFlow `WorkflowWorker`.

FerricStore remains the source of truth. The adapter restores the Spring machine from `WorkflowContext.state()` for each claimed job, sends one event, and converts the accepted target state into a FerricFlow outcome. Spring Statemachine persistence should not be configured as workflow persistence for this adapter.

Spring actions and guards receive the `FerricStoreClient`, `WorkflowContext`, and `FlowRecord` in message headers, so they can still use KV, JSON, streams, value refs, or raw commands.

When the Boot starter and statemachine adapter are both present, a single Spring `StateMachineFactory<String, String>` bean is enough for auto-configuration to expose a `FerricFlowStateMachine` bean.

Spring Statemachine reference: https://docs.spring.io/spring-statemachine/docs/current/reference/

## Throughput-Oriented Choices

The SDK keeps the hot path thin:

- no replay sandbox;
- no method proxy requirement;
- no generated wrappers around handlers;
- direct RESP commands through Jedis;
- batch APIs such as `createMany`, `completeMany`, `transitionMany`, `retryMany`, `failMany`, and `cancelMany`;
- value refs let workers hydrate only the named values they need.

The storage throughput story belongs mostly to FerricStore itself: FerricStore owns the storage path and FerricFlow state is stored inside FerricStore, not through a separate workflow database client from this SDK.
