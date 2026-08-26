#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_dir"

for format in json msgpack; do
  echo "Starting isolated authenticated TLS HTTP integration server for $format" >&2
  FERRICSTORE_HTTP_FORMAT="$format" scripts/run-http-integration.sh
done
