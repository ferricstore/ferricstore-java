# Release Process

Releases are published to Maven Central from GitHub Actions when a version tag is pushed.

## Prerequisites

- GitHub secrets for Maven Central are configured:
  - `CENTRAL_USERNAME`
  - `CENTRAL_PASSWORD`
  - `GPG_PRIVATE_KEY`
  - `GPG_PASSPHRASE`
- `pom.xml` versions and `CHANGELOG.md` are updated.
- `mise exec -- mvn test` passes locally.
- `mise exec -- mvn -DskipTests package` passes locally.

## Release Steps

1. Update the Maven project version.
2. Move the changelog section from `Unreleased` to the release date.
3. Commit the release change.
4. Create a signed tag:

   ```bash
   git tag -s v0.1.0 -m "v0.1.0"
   git push origin main --tags
   ```

5. GitHub Actions runs `mvn -P release deploy`.
6. GitHub Actions creates a GitHub release with generated release notes.

## Dry Run

```bash
mise exec -- mvn test
mise exec -- mvn -DskipTests package
```
