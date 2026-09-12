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
- `mise exec -- mvn -Pquality verify` passes locally.
- `mise exec -- mvn -DskipTests package` passes locally.

## Release Steps

1. Update the Maven project version.
2. Move the changelog section from `Unreleased` to the release date.
3. Commit the release change.
4. Create a signed tag:

   ```bash
   git tag -s v0.2.1 -m "v0.2.1"
   git push origin main --tags
   ```

5. GitHub Actions runs the quality gates and uploads one Central deployment.
6. The workflow polls that deployment ID until Central reports `PUBLISHED`.
7. The workflow resolves every public artifact from a fresh Maven repository.
8. GitHub Actions creates a GitHub release with generated release notes.

The upload and publication wait are deliberately separate. If Central processing
outlives the release runner, use the `recover central release` workflow with the
existing deployment ID and tag. Recovery only polls and verifies the accepted
deployment; it never uploads the immutable version again.

## Dry Run

```bash
mise exec -- mvn test
mise exec -- mvn -Pquality verify
mise exec -- mvn -DskipTests package
```
