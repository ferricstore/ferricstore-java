#!/usr/bin/env bash
set -euo pipefail

bundle_path="${1:-target/central-publishing/central-bundle.zip}"
expected_group_path="io/github/ferricstore"
expected_version="0.1.3"
expected_artifacts=(
  ferricstore-java-parent
  ferricstore-java
  ferricstore-spring-statemachine
  ferricstore-spring-boot-starter
)

if [[ ! -f "${bundle_path}" ]]; then
  echo "Release bundle not found: ${bundle_path}" >&2
  exit 1
fi

bundle_entries="$(unzip -Z1 "${bundle_path}")"

if grep -qE '(^|/)ferricstore-examples/' <<<"${bundle_entries}"; then
  echo "Release bundle must not publish ferricstore-examples" >&2
  exit 1
fi

unexpected_entries="$(
  awk -v prefix="${expected_group_path}/" 'index($0, prefix) != 1 { print }' <<<"${bundle_entries}"
)"
if [[ -n "${unexpected_entries}" ]]; then
  echo "Release bundle contains entries outside ${expected_group_path}:" >&2
  echo "${unexpected_entries}" >&2
  exit 1
fi

actual_artifacts="$(
  awk -F/ 'NF >= 5 { print $4 }' <<<"${bundle_entries}" | sort -u
)"
expected_artifact_list="$(printf '%s\n' "${expected_artifacts[@]}" | sort)"
if [[ "${actual_artifacts}" != "${expected_artifact_list}" ]]; then
  echo "Release bundle artifact set is incorrect" >&2
  echo "Expected:" >&2
  echo "${expected_artifact_list}" >&2
  echo "Actual:" >&2
  echo "${actual_artifacts}" >&2
  exit 1
fi

for artifact in "${expected_artifacts[@]}"; do
  pom_entry="${expected_group_path}/${artifact}/${expected_version}/${artifact}-${expected_version}.pom"
  if ! grep -qx "${pom_entry}" <<<"${bundle_entries}"; then
    echo "Release bundle is missing ${pom_entry}" >&2
    exit 1
  fi
  if ! grep -qx "${pom_entry}.asc" <<<"${bundle_entries}"; then
    echo "Release bundle is missing ${pom_entry}.asc" >&2
    exit 1
  fi
done

for artifact in "${expected_artifacts[@]:1}"; do
  artifact_prefix="${expected_group_path}/${artifact}/${expected_version}/${artifact}-${expected_version}"
  for suffix in .jar -sources.jar -javadoc.jar; do
    if ! grep -qx "${artifact_prefix}${suffix}" <<<"${bundle_entries}"; then
      echo "Release bundle is missing ${artifact_prefix}${suffix}" >&2
      exit 1
    fi
    if ! grep -qx "${artifact_prefix}${suffix}.asc" <<<"${bundle_entries}"; then
      echo "Release bundle is missing ${artifact_prefix}${suffix}.asc" >&2
      exit 1
    fi
  done
done

echo "Release bundle contains only the expected io.github.ferricstore artifacts"
