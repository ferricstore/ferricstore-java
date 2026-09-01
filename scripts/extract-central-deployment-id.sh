#!/usr/bin/env bash
set -euo pipefail

log_path="${1:-}"
if [[ -z "${log_path}" || ! -f "${log_path}" ]]; then
  echo "Central publishing log not found: ${log_path:-<empty>}" >&2
  exit 1
fi

deployment_ids="$({
  sed -nE \
    's/.*deploymentId: ([[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}).*/\1/p' \
    "${log_path}" | sort -u
})"

if [[ -z "${deployment_ids}" ]]; then
  echo "Central did not report a deployment ID after upload" >&2
  exit 1
fi
if [[ "${deployment_ids}" == *$'\n'* ]]; then
  echo "Central reported conflicting deployment IDs:" >&2
  echo "${deployment_ids}" >&2
  exit 1
fi

printf '%s\n' "${deployment_ids}"
