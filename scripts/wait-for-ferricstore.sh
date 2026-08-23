#!/usr/bin/env bash
set -euo pipefail

host="${FERRICSTORE_HOST:-127.0.0.1}"
port="${FERRICSTORE_PORT:-6388}"
deadline=$((SECONDS + ${FERRICSTORE_WAIT_SECONDS:-30}))
compose_container=""

if command -v docker >/dev/null 2>&1; then
  compose_container="$(docker compose ps -q ferricstore 2>/dev/null || true)"
fi

while (( SECONDS < deadline )); do
  runtime_ready=true
  if [ -n "$compose_container" ]; then
    runtime_ready=false
    if docker compose exec -T ferricstore bin/ferricstore rpc \
      'case Ferricstore.Health.ready?() do true -> :ready; false -> raise "not ready" end' \
      >/dev/null 2>&1; then
      runtime_ready=true
    fi
  fi
  if [ "$runtime_ready" = true ] && (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "FerricStore did not become reachable at ${host}:${port}" >&2
exit 1
