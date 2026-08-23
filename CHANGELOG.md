# Changelog

## Unreleased

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
