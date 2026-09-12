#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
if [[ ! "${version}" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]]; then
  echo "Invalid Maven release version: ${version:-<empty>}" >&2
  exit 1
fi

group_id="io.github.ferricstore"
artifacts=(
  "${group_id}:ferricstore-java-parent:${version}:pom"
  "${group_id}:ferricstore-java:${version}:jar"
  "${group_id}:ferricstore-spring-statemachine:${version}:jar"
  "${group_id}:ferricstore-spring-boot-starter:${version}:jar"
)
verification_dir="$(mktemp -d)"
trap 'rm -rf "${verification_dir}"' EXIT

for artifact in "${artifacts[@]}"; do
  echo "Resolving ${artifact} from Maven Central"
  (
    cd "${verification_dir}"
    mvn -B -q \
      -Dmaven.repo.local="${verification_dir}/repository" \
      org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get \
      -Dartifact="${artifact}" \
      -DremoteRepositories=central::default::https://repo.maven.apache.org/maven2
  )
done

echo "Maven Central ${version} resolves from a fresh local repository"
