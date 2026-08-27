package com.ferricstore.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;

/** Repeated, warm-up-aware runner for the real-server workflow benchmark. */
public final class ProtocolWorkflowBenchmarkSeries {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> SUMMARY_METRICS =
            List.of(
                    "workflow_completions_per_sec",
                    "state_actions_per_sec",
                    "create_flows_per_sec",
                    "client_cpu_seconds",
                    "client_cpu_percent",
                    "total_seconds",
                    "create_batch_latency_p50_ms",
                    "create_batch_latency_p95_ms",
                    "claim_latency_p50_ms",
                    "claim_latency_p95_ms",
                    "claim_latency_p99_ms",
                    "apply_latency_p50_ms",
                    "apply_latency_p95_ms",
                    "apply_latency_p99_ms");

    private ProtocolWorkflowBenchmarkSeries() {}

    @SuppressWarnings("PMD.SystemPrintln") // JSON is the benchmark's machine-readable output.
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        SeriesConfig series = SeriesConfig.parse(args);
        Map<String, Object> result = run(series);
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    @SuppressWarnings("PMD.SystemPrintln") // Progress belongs on stderr; stdout remains valid JSON.
    static Map<String, Object> run(SeriesConfig series) throws InterruptedException {
        ProtocolWorkflowBenchmark.Config measured =
                ProtocolWorkflowBenchmark.Config.parse(series.benchmarkArgs());
        ProtocolWorkflowBenchmark.Config warmup =
                ProtocolWorkflowBenchmark.Config.parse(
                        replaceOption(
                                series.benchmarkArgs(),
                                "flows",
                                Integer.toString(
                                        Math.min(series.warmupFlows(), measured.flows()))));

        for (int index = 0; index < series.warmupRuns(); index++) {
            System.err.printf(
                    "workflow benchmark warm-up %d/%d (%d flows)%n",
                    index + 1, series.warmupRuns(), warmup.flows());
            ProtocolWorkflowBenchmark.run(warmup);
        }

        List<Map<String, Object>> runs = new ArrayList<>(series.measurementRuns());
        Recording recording = startRecording(series.jfrFile());
        try {
            for (int index = 0; index < series.measurementRuns(); index++) {
                System.err.printf(
                        "workflow benchmark measurement %d/%d (%d flows)%n",
                        index + 1, series.measurementRuns(), measured.flows());
                Map<String, Object> result = ProtocolWorkflowBenchmark.run(measured);
                runs.add(result);
            }
        } finally {
            finishRecording(recording, series.jfrFile());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("benchmark", "java_protocol_workflow_series");
        result.put("sdk_version", runs.get(0).get("sdk_version"));
        result.put("java_version", runs.get(0).get("java_version"));
        result.put("transport", runs.get(0).get("transport"));
        result.put("http_format_requested", runs.get(0).get("http_format_requested"));
        result.put("url", runs.get(0).get("url"));
        result.put("warmup_runs", series.warmupRuns());
        result.put("warmup_flows", warmup.flows());
        result.put("measurement_runs", series.measurementRuns());
        result.put("jfr_file", series.jfrFile());
        result.put("flows_per_run", measured.flows());
        result.put("steps", measured.steps());
        result.put("workers", measured.workers());
        result.put("producers", measured.producers());
        result.put("runs", List.copyOf(runs));
        result.put("summary", summarize(runs));
        return result;
    }

    private static Recording startRecording(String jfrFile) {
        if (jfrFile == null) {
            return null;
        }
        try {
            Recording recording = new Recording(Configuration.getConfiguration("profile"));
            recording.setToDisk(true);
            recording.start();
            return recording;
        } catch (IOException | ParseException error) {
            throw new IllegalStateException("cannot start workflow benchmark JFR recording", error);
        }
    }

    private static void finishRecording(Recording recording, String jfrFile) {
        if (recording == null) {
            return;
        }
        try (recording) {
            recording.stop();
            recording.dump(Path.of(jfrFile));
        } catch (IOException error) {
            throw new IllegalStateException("cannot write workflow benchmark JFR recording", error);
        }
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // Each metric owns one result map.
    static Map<String, Object> summarize(List<Map<String, Object>> runs) {
        if (runs.isEmpty()) {
            throw new IllegalArgumentException("at least one measured run is required");
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        for (String metric : SUMMARY_METRICS) {
            double[] values =
                    runs.stream()
                            .map(run -> number(run, metric))
                            .mapToDouble(Number::doubleValue)
                            .sorted()
                            .toArray();
            double mean = Arrays.stream(values).average().orElseThrow();
            double variance =
                    Arrays.stream(values)
                            .map(
                                    value -> {
                                        double difference = value - mean;
                                        return difference * difference;
                                    })
                            .average()
                            .orElseThrow();
            Map<String, Object> metricSummary = new LinkedHashMap<>();
            metricSummary.put("median", median(values));
            metricSummary.put("mean", mean);
            metricSummary.put("min", values[0]);
            metricSummary.put("max", values[values.length - 1]);
            metricSummary.put("standard_deviation", Math.sqrt(variance));
            metricSummary.put(
                    "coefficient_of_variation_percent",
                    mean == 0.0 ? 0.0 : Math.sqrt(variance) / mean * 100.0);
            summary.put(metric, metricSummary);
        }
        return summary;
    }

    private static Number number(Map<String, Object> run, String metric) {
        Object value = run.get(metric);
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException("run does not contain numeric metric " + metric);
    }

    private static double median(double[] sorted) {
        int middle = sorted.length / 2;
        return sorted.length % 2 == 0
                ? (sorted[middle - 1] + sorted[middle]) / 2.0
                : sorted[middle];
    }

    private static String[] replaceOption(String[] args, String name, String value) {
        String option = "--" + name;
        String[] result = Arrays.copyOf(args, args.length + (contains(args, option) ? 0 : 2));
        for (int index = 0; index < args.length; index += 2) {
            if (option.equals(args[index])) {
                result[index + 1] = value;
                return result;
            }
        }
        result[args.length] = option;
        result[args.length + 1] = value;
        return result;
    }

    private static boolean contains(String[] args, String option) {
        for (int index = 0; index < args.length; index += 2) {
            if (option.equals(args[index])) {
                return true;
            }
        }
        return false;
    }

    record SeriesConfig(
            int warmupRuns,
            int warmupFlows,
            int measurementRuns,
            String jfrFile,
            String[] benchmarkArgs) {
        SeriesConfig {
            benchmarkArgs = Arrays.copyOf(benchmarkArgs, benchmarkArgs.length);
        }

        @Override
        public String[] benchmarkArgs() {
            return Arrays.copyOf(benchmarkArgs, benchmarkArgs.length);
        }

        static SeriesConfig parse(String[] args) {
            int warmupRuns = 1;
            int warmupFlows = 2_000;
            int measurementRuns = 5;
            String jfrFile = null;
            List<String> benchmarkArgs = new ArrayList<>();
            for (int index = 0; index < args.length; index += 2) {
                if (!args[index].startsWith("--") || index + 1 >= args.length) {
                    throw new IllegalArgumentException("invalid benchmark option: " + args[index]);
                }
                String value = args[index + 1];
                switch (args[index]) {
                    case "--warmup-runs" -> warmupRuns = nonNegative(value, args[index]);
                    case "--warmup-flows" -> warmupFlows = positive(value, args[index]);
                    case "--measurement-runs" -> measurementRuns = positive(value, args[index]);
                    case "--jfr-file" -> jfrFile = nonBlank(value, args[index]);
                    default -> {
                        benchmarkArgs.add(args[index]);
                        benchmarkArgs.add(value);
                    }
                }
            }
            ProtocolWorkflowBenchmark.Config.parse(benchmarkArgs.toArray(String[]::new));
            return new SeriesConfig(
                    warmupRuns,
                    warmupFlows,
                    measurementRuns,
                    jfrFile,
                    benchmarkArgs.toArray(String[]::new));
        }

        private static String nonBlank(String value, String option) {
            if (value.isBlank()) {
                throw new IllegalArgumentException(option + " must not be blank");
            }
            return value;
        }

        private static int positive(String value, String option) {
            int parsed = integer(value, option);
            if (parsed <= 0) {
                throw new IllegalArgumentException(option + " must be positive");
            }
            return parsed;
        }

        private static int nonNegative(String value, String option) {
            int parsed = integer(value, option);
            if (parsed < 0) {
                throw new IllegalArgumentException(option + " must be non-negative");
            }
            return parsed;
        }

        private static int integer(String value, String option) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException(option + " must be an integer", error);
            }
        }
    }
}
