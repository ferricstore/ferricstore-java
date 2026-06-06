# Python SDK Parity

This checklist tracks Java SDK parity against `ferricstore-python`.

Legend:

- Done: typed Java API exists.
- Partial: usable, but Python has richer ergonomics or scheduling controls.
- Raw: use `client.command(...)`.

## Core Client

| Python SDK Surface | Java Status | Notes |
| --- | --- | --- |
| `FlowClient.from_url` | Done | `FerricStoreClient.connect`. |
| `command` | Done | Raw RESP escape hatch. |
| `pipeline` | Done | Takes command arrays. |
| `close` | Done | Closes owned Jedis client. |
| `RawCodec` / `JsonCodec` / custom codec | Done | `Codec` interface. |
| Typed server error mapping | Done | Common FerricStore errors mapped. |
| Backpressure on producer writes | Raw | Retry policy can be built above `create`/`createMany`; Java does not yet have Python's producer backpressure controller. |
| `autobatch` | Raw | Use batch mutation helpers directly. |

## FerricStore Native Commands

| Command Family | Java Status |
| --- | --- |
| `CAS` | Done |
| `LOCK`, `UNLOCK`, `EXTEND` | Done |
| `RATELIMIT.ADD` | Done |
| `FETCH_OR_COMPUTE*` | Done |
| `FERRICSTORE.KEY_INFO` | Done |
| `FERRICSTORE.CONFIG`, `HOTNESS`, `METRICS`, `BLOBGC`, `DOCTOR` | Done |
| `CLUSTER.HEALTH`, `STATS`, `KEYSLOT`, `SLOTS`, `STATUS`, `ROLE`, `JOIN`, `LEAVE`, `FAILOVER`, `PROMOTE`, `DEMOTE` | Done |

## FerricFlow Commands

| Command | Java Status |
| --- | --- |
| `FLOW.CREATE` | Done |
| `FLOW.CREATE_MANY` | Done |
| `FLOW.VALUE.PUT` | Done |
| `FLOW.VALUE.MGET` | Done |
| `FLOW.SIGNAL` | Partial |
| `FLOW.CLAIM_DUE` | Done | Includes `claimDue` for records and `claimJobs` for compact lease items. |
| `FLOW.RECLAIM` | Done | Includes `reclaim` for records and `reclaimJobs` for compact lease items. |
| `FLOW.EXTEND_LEASE` | Done |
| `FLOW.TRANSITION`, `FLOW.TRANSITION_MANY` | Done |
| `FLOW.COMPLETE`, `FLOW.COMPLETE_MANY` | Done |
| `FLOW.RETRY`, `FLOW.RETRY_MANY` | Done |
| `FLOW.FAIL`, `FLOW.FAIL_MANY` | Done |
| `FLOW.CANCEL`, `FLOW.CANCEL_MANY` | Done |
| `FLOW.REWIND` | Done |
| `FLOW.GET`, `FLOW.LIST`, `FLOW.TERMINALS`, `FLOW.FAILURES`, `FLOW.STUCK` | Done |
| `FLOW.BY_PARENT`, `FLOW.BY_ROOT`, `FLOW.BY_CORRELATION` | Done |
| `FLOW.INFO`, `FLOW.HISTORY` | Done |
| `FLOW.SPAWN_CHILDREN` | Done |
| `FLOW.POLICY.SET`, `FLOW.POLICY.GET` | Done |
| `FLOW.RETENTION_CLEANUP` | Done |

## Queue API

| Python SDK Surface | Java Status | Notes |
| --- | --- | --- |
| `QueueClient.queue` | Done | `new QueueClient(client).queue(...)`. |
| `Queue.enqueue`, `enqueue_many` | Done | `enqueue`, `enqueueMany`. |
| Worker `run_once` | Done | `QueueWorker.runOnce` with `batchSize`, `concurrency`, virtual threads, or a custom `ExecutorService`. |
| Worker `run`, start/stop/join/stats | Partial | Java exposes concurrent `runOnce`; lifecycle loops belong in the application for now. |
| Exception policy retry/fail/raise | Partial | Java retries handler exceptions by default. |
| Advanced partition scanning/cooldowns | Raw | Use `ClaimDueOptions` directly for partition scans. |

## Workflow API

| Python SDK Surface | Java Status | Notes |
| --- | --- | --- |
| `WorkflowClient.workflow` | Done | Explicit state-machine workflow builder. |
| `Workflow.start` | Done | `Workflow.start`; use `client.createMany` for bulk starts. |
| `state(...)` registration | Done | `workflow.state(name, handler)`. |
| Outcomes `transition`, `complete`, `retry`, `fail` | Done | `Outcomes.*`. |
| `WorkflowContext.flow.*` helper object | Partial | Java exposes `ctx.client()` plus current job data. |
| Lazy value refs | Done | `ctx.value`. |
| Spawn children / fanout | Done | `client.spawnChildren`. |
| Class/decorator workflow style | Raw | Java intentionally starts with registration instead of annotation/proxy instrumentation. |
| Batch apply optimization | Partial | Low-level many commands exist; worker uniform batching is not automatic yet. |
| Worker lifecycle start/stop/join/stats | Partial | Java exposes concurrent `runOnce`; lifecycle loops belong in the application for now. |

## KV/Data-Structure Commands

| Redis-Compatible Family | Java Status |
| --- | --- |
| String/key/TTL commands | Done through `client.kv()`. |
| Hash commands | Done through `client.hash()`. |
| List commands | Done through `client.lists()`. |
| Set commands | Done through `client.sets()`. |
| Sorted set commands | Done through `client.zset()`. |
| Stream commands | Done through `client.stream()`. |
| Bitmap, HyperLogLog, Geo | Done through `client.bitmap()`, `client.hyperloglog()`, `client.geo()`. |
| Bloom, Cuckoo, Count-Min, TopK, TDigest | Done through `client.bloom()`, `client.cuckoo()`, `client.cms()`, `client.topk()`, `client.tdigest()`. |
| JSON | Done through `client.json()`. |

## Main Remaining Gaps

- Rich worker lifecycle and partition scheduler parity with Python.
- Producer backpressure/autobatch parity.
- More option coverage for `signal`, `history`, and read queries.
- Generated command matrix tests for every Redis-compatible helper and edge option.
- Async/reactive Java APIs.
