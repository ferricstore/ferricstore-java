# Contributing

Thanks for helping improve the FerricStore Java SDK.

## Development Setup

Artifacts target Java 17. Local quality tooling uses Java 21 because current Checkstyle and Error Prone releases require it; CI executes the SDK on Java 17, 21, and 25.

```bash
mise install
mise run test
mise run test:java17
```

The Maven reactor builds:

- `ferricstore-java`
- `ferricstore-spring-statemachine`
- `ferricstore-spring-boot-starter`
- `ferricstore-examples`

## Local FerricStore

For examples and integration testing:

```bash
docker compose up -d ferricstore
scripts/wait-for-ferricstore.sh
FERRICSTORE_INTEGRATION=1 mise exec -- mvn -pl ferricstore-java -am -Dtest=FerricStoreIntegrationTest test
docker compose down -v
```

## Design Rules

- Keep the command API independent from the native TCP/TLS and HTTP/HTTPS transport layers.
- Keep the core SDK framework-neutral; Spring modules are optional adapters.
- Keep worker polling application-controlled. Reusable sessions may own execution resources but must not create hidden global schedulers.
- Never close an application-supplied executor, and keep Java 21 features behind Java 17-safe adapters.
- Prefer explicit FerricFlow outcomes over replay, proxies, or hidden instrumentation.
- Preserve the escape hatch: anything missing from typed helpers must still work through `client.command(...)`.
- Add tests for command shape when adding a typed wrapper.
- Keep examples compile-checked.

## Quality Gates

Run the strict local gate before opening a release PR:

```bash
mise run quality
```

The `quality` profile fails on:

- compiler warnings with `-Xlint:all,-processing -Werror`
- Error Prone compile-time bug patterns
- Maven Enforcer dependency/build violations
- Spotless formatting drift
- Checkstyle source hygiene violations
- PMD correctness, security, performance, and concurrency violations
- SpotBugs findings at `Max` effort and `Low` threshold

## Pull Request Checklist

- Add or update tests.
- Update README/docs when changing public API.
- Run `mise run test:java17`.
- Run `mise run test`.
- Run `mise run quality`.
- Run `mise exec -- mvn -DskipTests package` for packaging changes.
