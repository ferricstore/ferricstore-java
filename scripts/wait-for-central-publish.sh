#!/usr/bin/env bash
set -euo pipefail

deployment_id="${1:-${CENTRAL_DEPLOYMENT_ID:-}}"
central_username="${CENTRAL_USERNAME:-}"
central_password="${CENTRAL_PASSWORD:-}"
wait_seconds="${CENTRAL_WAIT_SECONDS:-7200}"
poll_seconds="${CENTRAL_POLL_SECONDS:-15}"
status_endpoint="${CENTRAL_STATUS_ENDPOINT:-https://central.sonatype.com/api/v1/publisher/status}"

if [[ ! "${deployment_id}" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]; then
  echo "Invalid Central deployment ID: ${deployment_id:-<empty>}" >&2
  exit 1
fi
if [[ -z "${central_username}" || -z "${central_password}" ]]; then
  echo "CENTRAL_USERNAME and CENTRAL_PASSWORD are required" >&2
  exit 1
fi
if [[ ! "${wait_seconds}" =~ ^[0-9]+$ ]]; then
  echo "CENTRAL_WAIT_SECONDS must be a non-negative integer" >&2
  exit 1
fi
if [[ ! "${poll_seconds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "CENTRAL_POLL_SECONDS must be a positive integer" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to inspect the Central deployment status" >&2
  exit 1
fi

central_token="$(printf '%s:%s' "${central_username}" "${central_password}" | base64 | tr -d '\r\n')"
response_file="$(mktemp)"
trap 'rm -f "${response_file}"' EXIT
started_at="$(date +%s)"

while true; do
  http_status="$({
    printf 'header = "Authorization: Bearer %s"\n' "${central_token}" \
      | curl --silent --show-error \
        --config - \
        --output "${response_file}" \
        --write-out '%{http_code}' \
        --request POST \
        "${status_endpoint}?id=${deployment_id}"
  })"

  if [[ ! "${http_status}" =~ ^2[0-9][0-9]$ ]]; then
    echo "Central status request failed with HTTP ${http_status}" >&2
    jq -c . "${response_file}" >&2 2>/dev/null || true
    exit 1
  fi

  state="$(jq -er '.deploymentState | strings' "${response_file}")" || {
    echo "Central returned an invalid status response" >&2
    jq -c . "${response_file}" >&2 2>/dev/null || true
    exit 1
  }
  response_id="$(jq -r '.deploymentId // empty' "${response_file}")"
  if [[ -n "${response_id}" && "${response_id}" != "${deployment_id}" ]]; then
    echo "Central returned status for unexpected deployment ${response_id}" >&2
    exit 1
  fi

  echo "Central deployment ${deployment_id}: ${state}"
  case "${state}" in
    PUBLISHED)
      exit 0
      ;;
    FAILED)
      echo "Central deployment failed validation or publication:" >&2
      jq -c '.errors // []' "${response_file}" >&2
      exit 1
      ;;
    PENDING | VALIDATING | VALIDATED | PUBLISHING)
      ;;
    *)
      echo "Central returned unknown deployment state: ${state}" >&2
      exit 1
      ;;
  esac

  elapsed_seconds=$(( $(date +%s) - started_at ))
  if (( elapsed_seconds >= wait_seconds )); then
    echo "Timed out after ${wait_seconds}s waiting for Central deployment ${deployment_id}" >&2
    exit 1
  fi
  sleep "${poll_seconds}"
done
