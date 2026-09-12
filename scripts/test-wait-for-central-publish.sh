#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
wait_script="${script_dir}/wait-for-central-publish.sh"
deployment_id="64789a23-923d-4b2c-ab74-560210c41b98"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

curl() {
  local output_file=""
  while (( $# > 0 )); do
    if [[ "$1" == "--output" ]]; then
      output_file="$2"
      shift 2
    else
      shift
    fi
  done

  local invocation
  invocation="$(<"${MOCK_CURL_COUNTER}")"
  local -a states
  IFS=',' read -r -a states <<<"${MOCK_CENTRAL_STATES}"
  local state_index="${invocation}"
  if (( state_index >= ${#states[@]} )); then
    state_index=$((${#states[@]} - 1))
  fi
  local state="${states[${state_index}]}"
  printf '%s' "$((invocation + 1))" >"${MOCK_CURL_COUNTER}"

  if [[ "${MOCK_HTTP_STATUS:-200}" == "200" ]]; then
    if [[ "${state}" == "FAILED" ]]; then
      printf '{"deploymentId":"%s","deploymentState":"FAILED","errors":["bad signature"]}' \
        "${deployment_id}" >"${output_file}"
    else
      printf '{"deploymentId":"%s","deploymentState":"%s"}' \
        "${deployment_id}" "${state}" >"${output_file}"
    fi
  else
    printf '{"message":"unauthorized"}' >"${output_file}"
  fi
  printf '%s' "${MOCK_HTTP_STATUS:-200}"
}
export -f curl

sleep() {
  :
}
export -f sleep

run_wait() {
  local state_sequence="$1"
  local wait_seconds="${2:-30}"
  printf '0' >"${test_dir}/curl-counter"
  export MOCK_CURL_COUNTER="${test_dir}/curl-counter"
  export MOCK_CENTRAL_STATES="${state_sequence}"
  export MOCK_HTTP_STATUS=200

  set +e
  WAIT_OUTPUT="$({
    CENTRAL_USERNAME=user \
      CENTRAL_PASSWORD=password \
      CENTRAL_WAIT_SECONDS="${wait_seconds}" \
      CENTRAL_POLL_SECONDS=1 \
      "${wait_script}" "${deployment_id}"
  } 2>&1)"
  WAIT_STATUS=$?
  set -e
}

assert_contains() {
  local value="$1"
  local expected="$2"
  if [[ "${value}" != *"${expected}"* ]]; then
    echo "Expected output to contain '${expected}', got:" >&2
    echo "${value}" >&2
    exit 1
  fi
}

run_wait "PENDING,VALIDATING,PUBLISHING,PUBLISHED"
[[ "${WAIT_STATUS}" == "0" ]]
assert_contains "${WAIT_OUTPUT}" "PUBLISHED"
[[ "$(<"${MOCK_CURL_COUNTER}")" == "4" ]]

run_wait "FAILED"
[[ "${WAIT_STATUS}" != "0" ]]
assert_contains "${WAIT_OUTPUT}" "bad signature"

run_wait "PENDING" 0
[[ "${WAIT_STATUS}" != "0" ]]
assert_contains "${WAIT_OUTPUT}" "Timed out"

export MOCK_CURL_COUNTER="${test_dir}/curl-counter"
export MOCK_CENTRAL_STATES="PUBLISHED"
printf '0' >"${MOCK_CURL_COUNTER}"
set +e
invalid_output="$({
  CENTRAL_USERNAME=user CENTRAL_PASSWORD=password "${wait_script}" not-a-uuid
} 2>&1)"
invalid_status=$?
set -e
[[ "${invalid_status}" != "0" ]]
assert_contains "${invalid_output}" "Invalid Central deployment ID"
[[ "$(<"${MOCK_CURL_COUNTER}")" == "0" ]]

export MOCK_HTTP_STATUS=401
set +e
http_output="$({
  CENTRAL_USERNAME=user CENTRAL_PASSWORD=password "${wait_script}" "${deployment_id}"
} 2>&1)"
http_status=$?
set -e
[[ "${http_status}" != "0" ]]
assert_contains "${http_output}" "HTTP 401"

echo "Central publication polling tests passed"
