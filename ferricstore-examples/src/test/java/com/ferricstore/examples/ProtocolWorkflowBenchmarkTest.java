package com.ferricstore.examples;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ferricstore.ClaimDueOptions;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ProtocolWorkflowBenchmarkTest {
    @Test
    void defaultsMatchThePythonAsyncWorkflowShape() {
        ProtocolWorkflowBenchmark.Config config =
                ProtocolWorkflowBenchmark.Config.parse(new String[0]);

        assertEquals("live", config.shape());
        assertEquals(10_000, config.flows());
        assertEquals(3, config.steps());
        assertEquals(16, config.workers());
        assertEquals(4, config.producers());
        assertEquals(500, config.createBatchSize());
        assertEquals(32, config.createInflight());
        assertEquals(500, config.claimBatchSize());
        assertEquals("auto", config.partitionMode());
        assertEquals("dedicated", config.producerConnectionMode());
        assertEquals("dedicated", config.workerConnectionMode());
        assertEquals("atomic", config.mutationMode());
        assertEquals("json", config.httpFormat());
    }

    @Test
    void buildsEveryConfiguredWorkflowState() {
        assertEquals(List.of("queued"), ProtocolWorkflowBenchmark.workflowStates(1));
        assertEquals(
                List.of("queued", "step_1", "step_2"), ProtocolWorkflowBenchmark.workflowStates(3));
    }

    @Test
    void claimTargetsThePriorityUsedByEveryBenchmarkFlow() {
        ClaimDueOptions options =
                ProtocolWorkflowBenchmark.claimOptions(
                        "workflow", "worker-1", "queued", List.of(), 30_000, 500);

        assertEquals("queued", options.state());
        assertEquals(0L, options.priority());
        assertEquals(false, options.reclaimExpired());
    }

    @Test
    void createCommandIsBinarySafeAndSupportsExplicitPartitions() {
        byte[] payload = {0, (byte) 0xff};

        List<Object> command =
                ProtocolWorkflowBenchmark.createCommand(
                        "run", "type", 7, "queued", "run:partition:3", payload, 123L);

        assertEquals("FLOW.CREATE", command.get(0));
        assertEquals("run:flow:7", command.get(1));
        assertEquals("run:partition:3", command.get(command.indexOf("PARTITION") + 1));
        assertArrayEquals(payload, (byte[]) command.get(command.indexOf("PAYLOAD") + 1));
        assertEquals(123L, command.get(command.indexOf("NOW") + 1));
        assertEquals(123L, command.get(command.indexOf("RUN_AT") + 1));
    }

    @Test
    void rejectsUnsafeWorkflowShapes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolWorkflowBenchmark.Config.parse(new String[] {"--flows", "0"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolWorkflowBenchmark.Config.parse(new String[] {"--steps", "0"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmark.Config.parse(
                                new String[] {"--partition-mode", "unknown"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmark.Config.parse(
                                new String[] {"--worker-connection-mode", "unknown"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmark.Config.parse(
                                new String[] {"--producer-connection-mode", "unknown"}));
    }

    @Test
    void acceptsSingleMultiplexedConnectionShape() {
        ProtocolWorkflowBenchmark.Config config =
                ProtocolWorkflowBenchmark.Config.parse(
                        new String[] {
                            "--producers",
                            "1",
                            "--producer-connection-mode",
                            "shared",
                            "--worker-connection-mode",
                            "shared"
                        });

        assertEquals(1, config.producers());
        assertEquals("shared", config.producerConnectionMode());
        assertEquals("shared", config.workerConnectionMode());
    }

    @Test
    void supportsAtomicAndIndependentMutationWorkloads() {
        ProtocolWorkflowBenchmark.Config independent =
                ProtocolWorkflowBenchmark.Config.parse(
                        new String[] {"--mutation-mode", "independent"});

        assertEquals("independent", independent.mutationMode());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmark.Config.parse(
                                new String[] {"--mutation-mode", "unknown"}));
    }

    @Test
    void acceptsCompactHttpFormatAndRejectsUnknownFormats() {
        ProtocolWorkflowBenchmark.Config config =
                ProtocolWorkflowBenchmark.Config.parse(new String[] {"--http-format", "msgpack"});

        assertEquals("msgpack", config.httpFormat());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProtocolWorkflowBenchmark.Config.parse(
                                new String[] {"--http-format", "protobuf"}));
    }
}
