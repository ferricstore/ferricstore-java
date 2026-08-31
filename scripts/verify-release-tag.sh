#!/usr/bin/env bash
set -euo pipefail

expected_version="$(
  mvn -q -Dstyle.color=never help:evaluate \
    -Dexpression=project.version -DforceStdout 2>/dev/null \
    | tr -d '\r' \
    | tail -n 1
)"
release_tag="${RELEASE_TAG:-}"
if [[ -z "${release_tag}" && "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  release_tag="${GITHUB_REF_NAME:-}"
fi
if [[ -n "${release_tag}" && "${release_tag}" != "v${expected_version}" ]]; then
  echo "Release tag ${release_tag} does not match Maven version ${expected_version}" >&2
  exit 1
fi

echo "Release tag matches Maven version ${expected_version}"
