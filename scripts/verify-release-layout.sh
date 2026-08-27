#!/usr/bin/env bash
set -euo pipefail

expected_group_id="io.github.ferricstore"
expected_version="0.1.4"
published_modules=(
  ferricstore-java
  ferricstore-spring-statemachine
  ferricstore-spring-boot-starter
)
all_poms=(
  pom.xml
  ferricstore-java/pom.xml
  ferricstore-spring-statemachine/pom.xml
  ferricstore-spring-boot-starter/pom.xml
  ferricstore-examples/pom.xml
)

for pom in "${all_poms[@]}"; do
  group_id="$(
    mvn -q -Dstyle.color=never -f "${pom}" help:evaluate \
      -Dexpression=project.groupId -DforceStdout 2>/dev/null \
      | tr -d '\r' \
      | tail -n 1
  )"
  if [[ "${group_id}" != "${expected_group_id}" ]]; then
    echo "${pom} resolves to unexpected groupId: ${group_id}" >&2
    exit 1
  fi
done

if [[ -d ferricstore-examples/target ]]; then
  echo "Release reactor must not build ferricstore-examples" >&2
  exit 1
fi

for module in "${published_modules[@]}"; do
  artifact_prefix="${module}/target/${module}-${expected_version}"
  for suffix in .jar -sources.jar -javadoc.jar; do
    if [[ ! -f "${artifact_prefix}${suffix}" ]]; then
      echo "Release dry-run is missing ${artifact_prefix}${suffix}" >&2
      exit 1
    fi
  done
done

mvn -q -Dstyle.color=never help:effective-pom \
  -Doutput=target/release-effective-pom.xml

if ! grep -q '<waitUntil>published</waitUntil>' target/release-effective-pom.xml; then
  echo "Central release must wait until artifacts are published" >&2
  exit 1
fi

if ! grep -A3 '<excludeArtifacts>' target/release-effective-pom.xml \
  | grep -q '<excludeArtifact>ferricstore-examples</excludeArtifact>'; then
  echo "Central release must explicitly exclude ferricstore-examples" >&2
  exit 1
fi

echo "Release coordinates, reactor, artifacts, and Central safeguards are correct"
