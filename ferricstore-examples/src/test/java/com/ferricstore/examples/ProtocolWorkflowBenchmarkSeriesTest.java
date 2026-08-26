package com.ferricstore.examples;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class ProtocolWorkflowBenchmarkSeriesTest {
    @Test
    void defaultsProvideWarmupAndRepeatedMeasurements() {
        ProtocolWorkflowBenchmarkSeries.SeriesConfig config =
                ProtocolWorkflowBenchmarkSeries.SeriesConfig.parse(
                        new String[] {"--flows", "5000"});

        assertEquals(1, config.warmupRuns());
        assertEquals(2_000, config.warmupFlows());
        assertEquals(5, config.measurementRuns());
        assertNull(config.jfrFile());
        assertArrayEquals(new String[] {"--flows", "5000"}, config.benchmarkArgs());
    }

    @Test
    void separatesSeriesAndBenchmarkOptions() {
        ProtocolWorkflowBenchmarkSeries.SeriesConfig config =
                ProtocolWorkflowBenchmarkSeries.SeriesConfig.parse(
                        new String[] {
                            "--warmup-runs",
                            "2",
                            "--warmup-flows",
                            "750",
                            "--measurement-runs",
                            "7",
                            "--jfr-file",
                            "target/workflow.jfr",
                            "--workers",
                            "8"
                        });

        assertEquals(2, config.warmupRuns());
        assertEquals(750, config.warmupFlows());
        assertEquals(7, config.measurementRuns());
        assertEquals("target/workflow.jfr", config.jfrFile());
        assertArrayEquals(new String[] {"--workers", "8"}, config.benchmarkArgs());
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsMedianSpreadAndCoefficientOfVariation() {
        Map<String, Object> summary =
                ProtocolWorkflowBenchmarkSeries.summarize(List.of(run(10.0), run(20.0), run(30.0)));
        Map<String, Object> throughput =
                (Map<String, Object>) summary.get("workflow_completions_per_sec");

        assertEquals(20.0, throughput.get("median"));
        assertEquals(20.0, throughput.get("mean"));
        assertEquals(10.0, throughput.get("min"));
        assertEquals(30.0, throughput.get("max"));
        assertEquals(
                40.8248290463863,
                (double) throughput.get("coefficient_of_variation_percent"),
                1e-12);
    }

    @Test
    void rejectsInvalidSeriesShapesAndUnknownBenchmarkOptions() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmarkSeries.SeriesConfig.parse(
                                new String[] {"--measurement-runs", "0"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmarkSeries.SeriesConfig.parse(
                                new String[] {"--warmup-runs", "-1"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmarkSeries.SeriesConfig.parse(
                                new String[] {"--unknown", "1"}));
    }

    private static Map<String, Object> run(double value) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("workflow_completions_per_sec", value);
        result.put("state_actions_per_sec", value);
        result.put("create_flows_per_sec", value);
        result.put("client_cpu_seconds", value);
        result.put("client_cpu_percent", value);
        result.put("total_seconds", value);
        result.put("create_batch_latency_p50_ms", value);
        result.put("create_batch_latency_p95_ms", value);
        result.put("claim_latency_p50_ms", value);
        result.put("claim_latency_p95_ms", value);
        result.put("claim_latency_p99_ms", value);
        result.put("apply_latency_p50_ms", value);
        result.put("apply_latency_p95_ms", value);
        result.put("apply_latency_p99_ms", value);
        return result;
    }
}
