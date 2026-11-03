#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verify_script="${script_dir}/verify-release-bundle.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
unset GITHUB_REF_TYPE GITHUB_REF_NAME RELEASE_TAG

mvn() {
  printf '%s\n' 9.9.9
}
export -f mvn

group_path="io/github/ferricstore"
version="9.9.9"

write_bundle() {
  local bundle_dir="$1" signed="$2" artifact suffix file
  for artifact in ferricstore-java-parent ferricstore-java ferricstore-spring-statemachine ferricstore-spring-boot-starter; do
    mkdir -p "${bundle_dir}/${group_path}/${artifact}/${version}"
    for suffix in .pom .jar -sources.jar -javadoc.jar; do
      if [[ "${artifact}" == ferricstore-java-parent && "${suffix}" != .pom ]]; then
        continue
      fi
      file="${bundle_dir}/${group_path}/${artifact}/${version}/${artifact}-${version}${suffix}"
      : >"${file}"
      : >"${file}.sha1"
      if [[ "${signed}" == signed ]]; then
        : >"${file}.asc"
      fi
    done
  done
}

zip_bundle() {
  local bundle_dir="$1"
  (cd "${bundle_dir}" && zip -qrD bundle.zip io)
  printf '%s\n' "${bundle_dir}/bundle.zip"
}

expect_rejected() {
  local description="$1" bundle="$2" expected_message="$3" output
  if output="$("${verify_script}" "${bundle}" 2>&1)"; then
    echo "Expected ${description} to be rejected" >&2
    exit 1
  fi
  if ! grep -qF "${expected_message}" <<<"${output}"; then
    echo "Expected ${description} to be rejected with: ${expected_message}" >&2
    echo "${output}" >&2
    exit 1
  fi
}

# A complete signed bundle is accepted.
write_bundle "${test_dir}/good" signed
"${verify_script}" "$(zip_bundle "${test_dir}/good")" >/dev/null

# Maven 3.10 leaves local-repository metadata beside each artifact. Central
# rejects the whole deployment for it, so the bundle check has to as well.
write_bundle "${test_dir}/metadata" signed
: >"${test_dir}/metadata/${group_path}/ferricstore-java/maven-metadata-local.xml"
expect_rejected "artifact-level Maven metadata" "$(zip_bundle "${test_dir}/metadata")" \
  "${group_path}/ferricstore-java/maven-metadata-local.xml"

write_bundle "${test_dir}/tracking" signed
: >"${test_dir}/tracking/${group_path}/ferricstore-java/${version}/_remote.repositories"
expect_rejected "local repository tracking files" "$(zip_bundle "${test_dir}/tracking")" \
  "${group_path}/ferricstore-java/${version}/_remote.repositories"

# Signatures are required unless the caller builds an unsigned dry-run bundle.
write_bundle "${test_dir}/unsigned" unsigned
unsigned_bundle="$(zip_bundle "${test_dir}/unsigned")"
expect_rejected "an unsigned bundle" "${unsigned_bundle}" ".pom.asc"
RELEASE_BUNDLE_UNSIGNED=1 "${verify_script}" "${unsigned_bundle}" >/dev/null

echo "Release bundle verification tests passed"
