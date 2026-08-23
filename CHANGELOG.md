# Changelog

## Unreleased

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
