#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
extract_script="${script_dir}/extract-central-deployment-id.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
deployment_id="64789a23-923d-4b2c-ab74-560210c41b98"

printf '%s\n' \
  '[INFO] build output' \
  "[INFO] Uploaded bundle successfully, deployment name: Deployment, deploymentId: ${deployment_id}. Deployment will publish automatically" \
  >"${test_dir}/success.log"

actual_id="$("${extract_script}" "${test_dir}/success.log")"
[[ "${actual_id}" == "${deployment_id}" ]]

printf '%s\n' '[INFO] no deployment was uploaded' >"${test_dir}/missing.log"
if "${extract_script}" "${test_dir}/missing.log" >/dev/null 2>&1; then
  echo "Expected extraction to fail when the upload ID is absent" >&2
  exit 1
fi

printf '%s\n' \
  "[INFO] deploymentId: ${deployment_id}" \
  '[INFO] deploymentId: 11111111-2222-3333-4444-555555555555' \
  >"${test_dir}/ambiguous.log"
if "${extract_script}" "${test_dir}/ambiguous.log" >/dev/null 2>&1; then
  echo "Expected extraction to fail for conflicting deployment IDs" >&2
  exit 1
fi

echo "Central deployment ID extraction tests passed"
