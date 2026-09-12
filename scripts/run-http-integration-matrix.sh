#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_dir"

for version in h1 h2; do
  for format in json msgpack; do
    echo "Starting isolated authenticated TLS HTTP integration server for $format over $version" >&2
    FERRICSTORE_HTTP_FORMAT="$format" FERRICSTORE_HTTP_VERSION="$version" scripts/run-http-integration.sh
  done
done
