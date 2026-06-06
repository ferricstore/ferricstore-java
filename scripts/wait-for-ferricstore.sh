#!/usr/bin/env bash
set -euo pipefail

host="${FERRICSTORE_HOST:-127.0.0.1}"
port="${FERRICSTORE_PORT:-6379}"
deadline=$((SECONDS + ${FERRICSTORE_WAIT_SECONDS:-30}))

while (( SECONDS < deadline )); do
  if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "FerricStore did not become reachable at ${host}:${port}" >&2
exit 1
