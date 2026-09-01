# Changelog

## Unreleased

- Reject malformed-Unicode durable step names before closure execution or
  network I/O, preventing replacement-character collisions in journal keys.
- Enforce Maven 3.9 and Java 17 in the default reactor build while retaining the
  stricter Java 21 static-analysis profile used by maintainers.

## 0.2.0 - 2026-09-01

- Add chainable `advance()` and durable `step()` APIs that infer workflow identity,
  lease, fencing token, partition, and logical state from a claimed job and return
  the refreshed claim plus the journaled result.
- Add type-safe Java replay decoding, including default-codec strings, JSON POJOs,
  custom result decoders, and durable `Void` results.
- Add framework-neutral `stateAsync(...)` workflow handlers and genuinely composed
  `CompletableFuture` workers on Java 17, with guarded per-context mutations and
  recovery-required cancellation semantics.
- Preserve worker lease and reclaim behavior, add partition-scoped workers, and
  prevent response-loss, interruption, cancellation, or final-mutation failures
  from issuing stale fallback writes.
- Cover worker takeover before commit, provider idempotency, response loss after
  commit through a real transport disconnect, waiting-state release, and real
  worker-session termination over native TCP/TLS and authenticated HTTP/1.1 and
  HTTP/2.
- Classify ambiguous native and HTTP command failures conservatively and classify
  all native local preparation failures as definitely not sent.
- Treat HTTP request timeouts after dispatch as outcome-unknown durable mutations,
  and reject direct or caller-runs closure execution before application code can
  block a transport completion thread.
- Atomically seal workflow contexts before their final mutation, poison claims on
  externally timed-out/completed mutations, and keep cancelled task bodies tracked
  until they actually exit.
- Claim worker jobs in concurrency-sized waves up to the configured batch size and
  drain every accepted sibling task before a failed poll returns.

## 0.1.4 - 2026-08-27

- Cover the complete typed FerricStore and FerricFlow command API with rich-argument
  integration tests over native TCP and authenticated TLS HTTP on Java 17 and 21.
- Route named values, extended and mapped batch items, and all supported terminal
  Flow mutations through the OSS typed payload contract.
- Preserve native multiplexing while bounding pipeline memory and sequencing oversized
  pipeline batches, with deterministic cancellation and response-decoding behavior.
- Correct `returnRecord` compatibility for OSS servers and reject unsupported retry
  named-value mutations locally without removing the existing builder API.

## 0.1.3 - 2026-08-26

- Bound HTTP capacity waiters and native multiplexed requests with configurable
  fail-fast pending limits, including Spring Boot configuration.
- Preserve real Java 17/21 concurrency over native TCP/TLS and authenticated
  HTTP/HTTPS while preventing burst-driven client memory growth.
- Add an explicit candidate-image integration gate for validating the SDK
  against the FerricStore OSS release being prepared.

## 0.1.2 - 2026-08-24

- Exercise real shared-client concurrency against the pinned FerricStore OSS
  image on Java 17 and Java 21 over native TCP and authenticated TLS HTTP,
  including atomic contention, response correlation, simultaneous server-side
  HTTP blockers, out-of-order native replies, and blocked-lane isolation.

## 0.1.1 - 2026-08-23

- Run the complete HTTP-compatible Java integration surface against an
  authenticated TLS listener in pull-request and Maven Central release gates.
- Support blocking list, sorted-set, stream, replication-wait, and FerricFlow
  polling commands as long-lived HTTP requests with validated timeout budgets.
- Reject direct and `COMMAND_EXEC`-wrapped connection-affine commands locally,
  preserve the native transport boundary, and document HTTPS test setup.
- Keep temporary TLS material in an owner-only directory, delete the CA key
  before container start, and expose only the server certificate/key read-only.

## 0.1.0 - 2026-08-23

- Published the SDK and Spring integrations under `io.github.ferricstore` Maven coordinates.
- Built the Java SDK as a Maven multi-module repo.
- Added `ferricstore-java` core client with codecs, typed FerricFlow commands, FerricStore native helpers, and store helpers.
- Added explicit durable queue and workflow APIs.
- Added concurrent worker execution with virtual-thread and custom executor support.
- Added `ferricstore-spring-boot-starter` auto-configuration.
- Added optional `ferricstore-spring-statemachine` adapter for graph validation with FerricStore-only workflow persistence.
- Added compile-checked examples.
- Added unit and opt-in integration tests.
- Added Docker Compose, CI, release docs, and API docs generation.
