#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_dir"

for command in java mvn; do
  command -v "$command" >/dev/null || {
    echo "$command is required; run this benchmark through mise" >&2
    exit 1
  }
done

if [ "${FERRICSTORE_BENCHMARK_SKIP_BUILD:-false}" != "true" ]; then
  mvn -q -pl ferricstore-examples -am -DskipTests package
fi

if [ -n "${FERRICSTORE_BENCHMARK_CLASSPATH:-}" ]; then
  dependency_classpath="$FERRICSTORE_BENCHMARK_CLASSPATH"
else
  sdk_dependency_classpath="$(mvn -q -pl ferricstore-java dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
  example_dependency_classpath="$(mvn -q -pl ferricstore-examples dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
  dependency_classpath="$sdk_dependency_classpath:$example_dependency_classpath"
fi
runtime_classpath="ferricstore-examples/target/classes:ferricstore-java/target/classes:$dependency_classpath"

exec java -cp "$runtime_classpath" com.ferricstore.examples.ProtocolWorkflowBenchmarkSeries "$@"
