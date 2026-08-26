# Java SDK benchmarks

The KV benchmark drives the public asynchronous SDK API against a real FerricStore server. It
uses one client connection, bounded in-flight batches, verifies a binary SET/GET round trip before
measurement, drains every submitted request, and reports completed operations separately from
errors. Both Java 17 and Java 21 run the same code path.

Start the local server and run a short native baseline:

```bash
docker compose up -d
mise run benchmark:tcp -- --preset get-latency --duration-seconds 10
```

Run the Python-compatible throughput shapes:

```bash
mise run benchmark:tcp -- --preset get-throughput
mise run benchmark:tcp -- --preset set-throughput
```

Run against an authenticated HTTPS endpoint:

```bash
FERRICSTORE_HTTP_URL=https://127.0.0.1:8080 \
FERRICSTORE_USERNAME=sdk-http \
FERRICSTORE_PASSWORD=sdk-http-secret \
mise run benchmark:http -- --preset get-throughput --http-version 1.1
```

For a private CA, provide it through the JVM trust store, for example with
`JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=/path/to/truststore.p12` and the corresponding
`javax.net.ssl.trustStorePassword` setting.

The named shapes match the Python SDK benchmark's batch, in-flight, key-count, value-size, and
duration settings. The implementation is intentionally transport-neutral: native TCP multiplexes
the commands on its single socket, while HTTP sends one command array per request and reuses the
HTTP connection.

Always record the Java SDK commit, FerricStore image digest, Java version, host CPU, server shard
count, transport and HTTP version, exact command, and whether the data directory was clean. Do not
compare results from a dirty or concurrently used server.

## Workflow benchmark

The workflow benchmark matches the Python SDK's default live state-machine shape: 10,000 flows,
three states, 16 workers, four producers, 16 automatic partitions, 500-item create and claim
batches, 32 in-flight create batches, no payload, and atomic per-shard many-item mutations. Every
generated flow uses priority 0, so claims explicitly target priority 0 instead of scanning unused
priority indexes. It runs the real `FLOW.CREATE`, `FLOW.CLAIM_DUE`, `FLOW.TRANSITION_MANY`,
`FLOW.COMPLETE_MANY`, and sampled `FLOW.GET` lifecycle. A run fails unless all 10,000 flows complete,
exactly 30,000 state actions are claimed, and every sampled record is terminal.

```bash
docker compose up -d ferricstore
mise run benchmark:workflow:tcp:java21

FERRICSTORE_HTTP_URL=https://127.0.0.1:8080 \
FERRICSTORE_USERNAME=sdk-http \
FERRICSTORE_PASSWORD=sdk-http-secret \
mise run benchmark:workflow:http:java21
```

Atomic mutation mode is the throughput default because freshly claimed benchmark items are valid
and it exercises the server's true batched transition and completion apply paths. Use
`--mutation-mode independent` to benchmark per-item failure isolation instead; the JSON output
records the selected mode, and lower throughput is expected because the server must preserve an
individual result for every item.

The default topology deliberately mirrors the Python benchmark: four producer clients and separate
claim/apply clients per worker. To measure the native protocol's true one-socket multiplexing path,
run:

```bash
mise run benchmark:workflow:tcp:java21 -- \
  --producers 1 \
  --producer-connection-mode shared \
  --worker-connection-mode shared
```

For regression-quality measurements, use the isolated matrix runner instead of selecting one short
sample:

```bash
mise run benchmark:workflow:matrix
```

It builds once and runs five independently isolated samples for each Java 17/21 native, HTTP JSON,
and HTTP MessagePack scenario. Every sample starts a fresh pinned FerricStore container, waits until
all 16 shards and the health endpoint are ready, performs an excluded JVM warm-up, and then measures
10,000 flows. This keeps the storage state identical across samples and prevents completed records
from depressing later results. Every sample must create and complete all workflows, claim exactly
three actions per workflow, and pass sampled terminal-state verification. The JSON artifacts include
every run plus median, mean, min, max, standard deviation, and coefficient of variation for
throughput, CPU, and latency. Override the sample count with `FERRICSTORE_BENCHMARK_SAMPLES=7`;
benchmark options remain available after `--`, for example
`mise run benchmark:workflow:matrix -- --warmup-flows 5000`.

For a measurement-only Java Flight Recorder profile, start the recording after the excluded warm-up
instead of at JVM startup:

```bash
mise run benchmark:workflow:series:http:java21 -- \
  --http-format msgpack \
  --warmup-flows 5000 \
  --flows 50000 \
  --measurement-runs 1 \
  --jfr-file target/http-msgpack-measurement-only.jfr
```

The JSON result records the JFR path. This avoids attributing warm-up class loading and TLS setup to
the steady-state command path.

### Compact HTTP comparison

The final August 24 matrix used five fresh-server samples per scenario after all retained client
changes. Each row completed 50,000 measured workflows in total with no command or verification
errors. Throughput is the median workflow completion rate; CV is its coefficient of variation.

| Runtime | Transport | Median workflows/s | Throughput CV | Median client CPU |
| --- | --- | ---: | ---: | ---: |
| Java 21 | native | 2,078 | 4.07% | 0.552 s |
| Java 17 | native | 2,140 | 4.25% | 0.696 s |
| Java 21 | HTTP JSON | 2,076 | 6.72% | 0.989 s |
| Java 21 | HTTP MessagePack | 2,012 | 11.32% | 1.029 s |
| Java 17 | HTTP JSON | 2,125 | 11.26% | 1.356 s |
| Java 17 | HTTP MessagePack | 2,130 | 10.77% | 1.250 s |

The short HTTP samples were noisy enough that they do not support a MessagePack throughput or CPU
claim: compact throughput differed by -3.1% on Java 21 and +0.2% on Java 17, while median CPU
differed by +4.0% and -7.8%, respectively. Longer 50,000-workflow Java 21 profiles better isolated
encoding cost at essentially identical throughput (1,890 workflows/s): JSON used 3.395 seconds of
client CPU and MessagePack used 2.852 seconds, a 16.0% reduction. An earlier long profile also found
that compact HTTP reduced estimated Java allocation from about 287 MB to 174 MB (39%). Compact
decoding removes JSON Base64 work and restores successful response values while unpacking, without
an intermediate text value or a second response-tree copy.

JSON remains the compatibility default. Enable the compact FerricStore envelope explicitly with
`HttpTransportOptions.builder().compact(true)` or benchmark it with `--http-format msgpack`. A
custom MessagePack request-buffer experiment was not retained: it removed individual visible copy
sites, but total profiled allocation remained effectively unchanged and client CPU did not improve.

The final create-path pass also avoided rebuilding identical metadata for every item in a
homogeneous `FLOW.CREATE` pipeline. Later commands compare type and state against the first command
with a strict allocation-free UTF-8 check and reuse its validated bytes. JSON and MessagePack then
stream the fixed eight-field `FLOW.CREATE_MANY` payload directly while preserving the distinction
between an absent payload and an explicitly empty payload. In a controlled 50,000-workflow JSON
A/B, the direct envelope kept create throughput flat (19,151 versus 19,343 flows/s) and reduced
client CPU from 4.098 to 3.395 seconds (17.1%). The equivalent retained compact changes reduced
client CPU from 3.458 to 2.852 seconds (17.5%) in the long profile. A broader fixed-protocol-string
lookup and a specialized 19-field many-mutation writer were both rejected because their measured
benefit did not justify their lookup or maintenance cost.

The following short local diagnostics used the same pinned 0.11.11 image and host as the KV
baseline. Java and Python used the same 10,000-flow shape. The HTTP listener used authenticated
plain HTTP/1.1 to isolate gateway/client overhead; the separate authenticated TLS integration suite
remained green. Every retained Java row completed 10,000 flows, executed 30,000 actions, and had
zero errors or verification failures.

| Runtime | Transport / topology | Workflow completions/s | State actions/s | Create flows/s |
| --- | --- | ---: | ---: | ---: |
| Python 3.10 | native, Python default | 2,344 | 7,033 | 18,936 |
| Java 21 | native, Python-equivalent | 2,318 | 6,954 | 36,598 |
| Java 17 | native, Python-equivalent | 2,308 | 6,923 | 23,486 |
| Java 21 | native, one client / one socket | 1,677 | 5,031 | 13,099 |
| Java 21 | HTTP/1.1, Python-equivalent clients | 2,284 | 6,851 | 23,780 |
| Java 17 | HTTP/1.1, Python-equivalent clients | 2,243 | 6,729 | 24,504 |

The benchmark exposed two native SDK bottlenecks and drove their fixes. Compatible homogeneous
`FLOW.CREATE` pipelines now use one compact `FLOW.CREATE_MANY` request with exact binary-safe
semantics; unsupported option shapes retain the generic pipeline path. Independent claim workers
and many-item mutations are routed across independent native lanes, allowing request-ID
multiplexing to perform useful concurrent work on one socket instead of serializing every worker on
one lane. Homogeneous HTTP create pipelines now use the same structured `FLOW.CREATE_MANY` server
operation rather than executing 500 individual creates inside the gateway. HTTP and native
`FLOW.COMPLETE_MANY` and `FLOW.TRANSITION_MANY` use strict typed payloads; compatible native shapes
also use their compact request codecs. The benchmark requests `RETURN OK_ON_SUCCESS`, avoiding
large record responses that no worker consumes.

These changes moved HTTP/1.1 from 778 to 2,284 workflow completions/s on Java 21 and from 803 to
2,243/s on Java 17. HTTP creation fell from roughly 12 seconds to 0.41–0.42 seconds for all 10,000
flows. An earlier 32-worker diagnostic reached 2,210/s, used 69 clients, and materially increased
latency and CPU; the optimized 16-worker shape is now faster and remains the balanced default.

A second profile-driven pass removed the final HTTP request-buffer copy, writes fixed-length
responses directly into one bounded byte array, retains a bounded merge path for chunked responses,
and sizes request buffers from item/argument volume. Sampled `Arrays.copyOf(byte[], int)` allocation
pressure fell from 26.6% to 8.0%. Native bulk Flow requests now validate and write strings directly
into the final compact frame through one shared strict UTF-8 primitive; this removed per-item
encoder and temporary-byte-array allocations and lowered profiled native client CPU by about 4%.
Compact claim rows are decoded into a pre-sized result list while preserving their unmodifiable
result contract.

A third steady-state pass used 50,000 flows and 150,000 verified state actions to separate
class-loading noise from the HTTP hot path. HTTP responses now use a binary-envelope-aware
streaming decoder, avoiding the generic Jackson object tree and the second recursive normalization
copy. The SDK also publishes its immutable request buffer directly instead of letting the JDK copy
it into another byte array, sizes structured request buffers from their actual argument content,
and reuses canonical immutable empty collections for compact claimed jobs. The retained profiles
contained no request-publisher array-copy samples and no request-buffer growth samples. In the
direct before/after publisher comparison, client CPU fell from 4.77 to 4.69 seconds; throughput
remained server-bound within short-run variance. Both runs completed all 50,000 workflows with
zero errors or verification failures.

The final isolated native runs reached 2,318/s on Java 21 and 2,308/s on Java 17, versus an exact
clean 2,069/s Java 21 baseline at the start of the second pass. HTTP and native are now within about
1–3% on this short local workflow shape. Compact claim specialization was tested twice and not
retained. The later implementation decoded MessagePack claim rows directly into typed jobs while
keeping raw commands binary-safe and falling back for other response shapes. In a controlled A/B
of three fresh-server, 50,000-workflow samples per path, mean throughput changed by only +0.3%
(1,957 to 1,963 workflows/s), while mean client CPU increased by 1.7% (2.337 to 2.376 seconds).
Median throughput fell 1.1% and median CPU increased 0.5%. The generic decoder therefore remains
the simpler and at least equally fast implementation.

## Diagnostic baseline: August 24, 2026

These are short local diagnostic runs, not general product claims. The host was an Apple M4 Max
with 128 GiB RAM. Server and client ran on the same host using
`quay.io/ferricstore/ferricstore:0.11.11` pinned to digest
`sha256:d9f488539f0d6c1a513d2315e7a9c2947cc795b393f3774c9de8ba5e5b5c21b5`. Each measurement
ran for five seconds after a correctness probe; every successful row had zero command errors.

| Runtime | Transport | Workload | Batch / in flight | Completed commands/s |
| --- | --- | --- | ---: | ---: |
| Java 21 | native | GET | 1,000 / 64 | 4.61M |
| Java 17 | native | GET | 1,000 / 64 | 4.61M |
| Java 21 | native | SET | 500 / 64 | 249k |
| Java 21 | HTTPS 1.1 | GET | 1,000 / 64 | 1.27M |
| Java 17 | HTTPS 1.1 | GET | 1,000 / 64 | 1.13M |
| Java 21 | HTTPS 1.1 | SET | 500 / 64 | 7.2k |

The HTTPS runs used authenticated TLS and 100 hot keys to keep setup outside the timed section.
The native GET comparison used 10,000 hot keys. A same-server Python compact-pipeline run reached
6.49M GET commands/s; it uses a benchmark-specific prebuilt wire-key path, while Java measures the
public command-list API including command construction and UTF-8 encoding.

The benchmark directly led to one SDK correction and one server optimization target:

- Native Java pipelines previously expanded into individual frames and could exceed the server's
  4,096-request connection window. Homogeneous GET/SET pipelines now use FerricStore's compact
  native `PIPELINE` frame, while mixed and structured command batches use one typed pipeline frame.
- The current HTTP gateway executes commands inside one JSON request individually. Its GET path is
  healthy, but durable SET throughput remains far below the native compact batch path. The next
  server-side performance work should add a semantics-preserving homogeneous KV batch fast path at
  the shared OSS command gateway.

Sustained HTTP/2 tests against this image produced occasional EOF failures on both Java 17 and 21;
HTTP/1.1 completed the same tests without errors. Do not hide this with generic SDK retries because
a failed write may already have executed. Keep HTTP/1.1 as the SDK default until the HTTP/2
connection lifecycle is corrected and covered by a long multiplexed-stream regression test.

## SDK optimization pass

A controlled follow-up used the same pinned image and five-second Java 21 GET workload before and
after each SDK change. The native server was reset before the comparison. To isolate JSON/client
cost from TLS, the HTTP comparison used one authenticated plain HTTP/1.1 listener; the separate
TLS integration matrix remained green. All retained measurements completed with zero errors.

| Path | Before | After | Change |
| --- | ---: | ---: | ---: |
| Native GET, 1,000 / 64 | 3.66M commands/s | 4.50M commands/s | +23% |
| Native GET p95 batch latency | 76.5 ms | 19.1 ms | -75% |
| HTTP/1.1 GET, 1,000 / 64 | 1.16M commands/s | 1.31M commands/s | +13% |
| HTTP/1.1 GET p50 batch latency | 52.0 ms | 46.7 ms | -10% |
| Native GET response codec A/B | 3.66M typed/s | 3.81M compact/s | +4% |

The native improvement replaces per-key UTF-8 arrays and a full payload copy with exact-size direct
wire encoding, avoids an extra command-list copy, and retains case-insensitive string and binary
command names. The negotiated compact pipeline response codec added another 4% in a clean
same-host A/B and lowered p95 latency; it remains enabled. The HTTP improvement streams the
binary-safe JSON envelope directly instead of building temporary maps, lists, and Base64 strings
for every command.

A native batch/depth grid found that 500–1,000 commands per compact frame is the throughput range
on this host. Thirty-two batches in flight materially lowers tail latency; 64 provides a small
throughput gain at the cost of roughly doubling batch latency. Applications should choose the
depth according to their latency budget rather than treating 64 as a universal default.
