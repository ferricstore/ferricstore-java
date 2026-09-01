#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verify_script="${script_dir}/verify-maven-central-release.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
export MOCK_MAVEN_CALLS="${test_dir}/maven-calls"
: >"${MOCK_MAVEN_CALLS}"

mvn() {
  local argument
  for argument in "$@"; do
    if [[ "${argument}" == -Dartifact=* ]]; then
      printf '%s\n' "${argument#-Dartifact=}" >>"${MOCK_MAVEN_CALLS}"
    fi
  done
}
export -f mvn

"${verify_script}" 0.2.0

expected_calls="$({
  printf '%s\n' \
    'io.github.ferricstore:ferricstore-java-parent:0.2.0:pom' \
    'io.github.ferricstore:ferricstore-java:0.2.0:jar' \
    'io.github.ferricstore:ferricstore-spring-statemachine:0.2.0:jar' \
    'io.github.ferricstore:ferricstore-spring-boot-starter:0.2.0:jar'
})"
actual_calls="$(<"${MOCK_MAVEN_CALLS}")"
[[ "${actual_calls}" == "${expected_calls}" ]]

if "${verify_script}" '../invalid' >/dev/null 2>&1; then
  echo "Expected an unsafe Maven version to be rejected" >&2
  exit 1
fi

echo "Maven Central release verification tests passed"
