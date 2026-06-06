# Contributing

Thanks for helping improve the FerricStore Java SDK.

## Development Setup

Use Java 21 and Maven.

```bash
mise install
mise exec -- mvn test
```

The Maven reactor builds:

- `ferricstore-java`
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

- Keep the SDK thin over FerricStore RESP commands.
- Prefer explicit FerricFlow outcomes over replay, proxies, or hidden instrumentation.
- Preserve the escape hatch: anything missing from typed helpers must still work through `client.command(...)`.
- Add tests for command shape when adding a typed wrapper.
- Keep examples compile-checked.

## Pull Request Checklist

- Add or update tests.
- Update README/docs when changing public API.
- Run `mise exec -- mvn test`.
- Run `mise exec -- mvn -DskipTests package` for packaging changes.
