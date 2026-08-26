# Changelog

## Unreleased

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
