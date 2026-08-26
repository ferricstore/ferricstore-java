#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_dir"

image="${FERRICSTORE_IMAGE:-quay.io/ferricstore/ferricstore:0.11.11@sha256:d9f488539f0d6c1a513d2315e7a9c2947cc795b393f3774c9de8ba5e5b5c21b5}"
scenario_list="${FERRICSTORE_BENCHMARK_SCENARIOS:-java21-native,java17-native,java21-http,java17-http,java21-http-msgpack,java17-http-msgpack}"
output_dir="${FERRICSTORE_BENCHMARK_OUTPUT_DIR:-$repo_dir/target/workflow-benchmark-$(date -u +%Y%m%dT%H%M%SZ)}"
samples="${FERRICSTORE_BENCHMARK_SAMPLES:-5}"
username="sdk-http"
password="sdk-http-secret"
active_container=""

cleanup() {
  status=$?
  if [ -n "$active_container" ]; then
    if [ "$status" -ne 0 ]; then
      docker logs "$active_container" 2>/dev/null || true
    fi
    docker stop "$active_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

for command in curl docker jq mise mvn; do
  command -v "$command" >/dev/null || {
    echo "$command is required" >&2
    exit 1
  }
done

case "$samples" in
  ''|*[!0-9]*|0)
    echo "FERRICSTORE_BENCHMARK_SAMPLES must be a positive integer" >&2
    exit 1
    ;;
esac

mkdir -p "$output_dir"
mise exec java@temurin-21 -- mvn -q -pl ferricstore-examples -am -DskipTests package
sdk_dependency_classpath="$(mise exec java@temurin-21 -- mvn -q -pl ferricstore-java dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
example_dependency_classpath="$(mise exec java@temurin-21 -- mvn -q -pl ferricstore-examples dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
benchmark_classpath="$sdk_dependency_classpath:$example_dependency_classpath"

start_server() {
  local server_label=$1
  active_container="ferricstore-java-workflow-benchmark-${server_label}-$$"
  docker run --detach --rm --name "$active_container" \
    -p 127.0.0.1::6388 \
    -p 127.0.0.1::8080 \
    -e FERRICSTORE_PROTECTED_MODE=false \
    -e FERRICSTORE_SHARD_COUNT=16 \
    -e FERRICSTORE_FLOW_SCHEDULER_ENABLED=false \
    -e FERRICSTORE_AUTH_RATE_LIMIT_MAX_ATTEMPTS=100000 \
    -e FERRICSTORE_HTTP_ENABLED=true \
    -e FERRICSTORE_HTTP_BIND=0.0.0.0 \
    -e FERRICSTORE_HTTP_PORT=8080 \
    -e FERRICSTORE_HTTP2_ENABLED=false \
    -e FERRICSTORE_HTTP_TLS_ENABLED=false \
    -e FERRICSTORE_HTTP_AUTH_CACHE_ENABLED=true \
    -e FERRICSTORE_HTTP_AUTH_CACHE_TTL_MS=300000 \
    -e FERRICSTORE_HTTP_AUTH_CACHE_MAX_ENTRIES=10000 \
    "$image" >/dev/null

  native_port="$(docker port "$active_container" 6388/tcp | awk -F: 'NR == 1 {print $NF}')"
  http_port="$(docker port "$active_container" 8080/tcp | awk -F: 'NR == 1 {print $NF}')"
  ready=false
  for _ in {1..240}; do
    shard_count="$(docker logs "$active_container" 2>&1 | grep -c 'is live after heartbeat quorum' || true)"
    if [ "$shard_count" -ge 16 ] && curl --silent --fail "http://127.0.0.1:$http_port/health" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 0.25
  done
  if [ "$ready" != true ]; then
    echo "FerricStore failed readiness for $server_label (ready shards: ${shard_count:-0})" >&2
    exit 1
  fi
}

stop_server() {
  docker stop "$active_container" >/dev/null
  active_container=""
}

verify_result() {
  local result_file=$1
  jq -e '
    .measurement_runs == (.runs | length) and
    (.runs | all(
      .errors == 0 and
      .verification_errors == 0 and
      .created == .flows and
      .completed == .flows and
      .claimed_actions == .expected_actions
    ))
  ' "$result_file" >/dev/null
}

aggregate_results() {
  local destination=$1
  shift
  jq -s '
    def stats:
      sort as $values |
      ($values | length) as $count |
      ($values | add / $count) as $mean |
      (($values |
        if $count % 2 == 0 then
          (.[($count / 2) - 1] + .[$count / 2]) / 2
        else
          .[($count / 2 | floor)]
        end)) as $median |
      (($values | map((. - $mean) * (. - $mean)) | add / $count) | sqrt) as $deviation |
      {
        median: $median,
        mean: $mean,
        min: $values[0],
        max: $values[-1],
        standard_deviation: $deviation,
        coefficient_of_variation_percent:
          (if $mean == 0 then 0 else $deviation / $mean * 100 end)
      };
    .[0] as $first |
    [.[].runs[]] as $runs |
    ($first.summary | keys) as $metrics |
    $first |
    .measurement_runs = ($runs | length) |
    .runs = $runs |
    .summary = reduce $metrics[] as $metric
      ({}; .[$metric] = ([$runs[] | .[$metric]] | stats))
  ' "$@" >"$destination"
}

IFS=',' read -r -a scenarios <<< "$scenario_list"
configure_scenario() {
  local scenario_name=$1
  case "$scenario_name" in
    java21-native) runtime="temurin-21"; transport="native"; http_format="" ;;
    java17-native) runtime="temurin-17"; transport="native"; http_format="" ;;
    java21-http) runtime="temurin-21"; transport="http"; http_format="json" ;;
    java17-http) runtime="temurin-17"; transport="http"; http_format="json" ;;
    java21-http-msgpack) runtime="temurin-21"; transport="http"; http_format="msgpack" ;;
    java17-http-msgpack) runtime="temurin-17"; transport="http"; http_format="msgpack" ;;
    *)
      echo "unknown benchmark scenario: $scenario_name" >&2
      exit 1
      ;;
  esac
}

for scenario in "${scenarios[@]}"; do
  configure_scenario "$scenario"
done

for ((sample = 1; sample <= samples; sample++)); do
  for scenario in "${scenarios[@]}"; do
    configure_scenario "$scenario"
    echo "Starting isolated $scenario sample $sample/$samples" >&2
    start_server "$scenario-$sample"
    sample_file="$output_dir/$scenario-sample-$sample.json"
    if [ "$transport" = "http" ]; then
      docker exec "$active_container" bin/ferricstore rpc \
        'case FerricstoreServer.Acl.set_user("sdk-http", ["on", "resetpass", ">sdk-http-secret", "resetkeys", "+@all", "~*", "&*"]) do :ok -> :ok; other -> raise "ACL bootstrap failed: #{inspect(other)}" end' >/dev/null
      env \
        FERRICSTORE_BENCHMARK_SKIP_BUILD=true \
        FERRICSTORE_BENCHMARK_CLASSPATH="$benchmark_classpath" \
        FERRICSTORE_HTTP_URL="http://127.0.0.1:$http_port" \
        FERRICSTORE_USERNAME="$username" \
        FERRICSTORE_PASSWORD="$password" \
        mise exec "java@$runtime" -- scripts/run-workflow-benchmark-series.sh \
          --url "http://127.0.0.1:$http_port" --http-version 1.1 \
          --http-format "$http_format" "$@" \
          --measurement-runs 1 >"$sample_file"
    else
      env \
        FERRICSTORE_BENCHMARK_SKIP_BUILD=true \
        FERRICSTORE_BENCHMARK_CLASSPATH="$benchmark_classpath" \
        FERRICSTORE_URL="ferric://127.0.0.1:$native_port" \
        mise exec "java@$runtime" -- scripts/run-workflow-benchmark-series.sh \
          --url "ferric://127.0.0.1:$native_port" "$@" \
          --measurement-runs 1 >"$sample_file"
    fi
    verify_result "$sample_file"
    stop_server
  done
done

for scenario in "${scenarios[@]}"; do
  configure_scenario "$scenario"
  sample_files=()
  for ((sample = 1; sample <= samples; sample++)); do
    sample_files+=("$output_dir/$scenario-sample-$sample.json")
  done
  result_file="$output_dir/$scenario.json"
  aggregate_results "$result_file" "${sample_files[@]}"
  verify_result "$result_file"
  jq -r '
    [
      .java_version,
      .transport,
      (.http_format_requested // "native"),
      (.summary.workflow_completions_per_sec.median | tostring),
      (.summary.workflow_completions_per_sec.coefficient_of_variation_percent | tostring),
      (.summary.client_cpu_seconds.median | tostring),
      (.summary.claim_latency_p95_ms.median | tostring),
      (.summary.apply_latency_p95_ms.median | tostring)
    ] | @tsv
  ' "$result_file"
done

echo "Benchmark JSON written to $output_dir" >&2
