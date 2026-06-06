package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class FerricStoreWorkerTest {
    @Test
    void queueWorkerProcessesClaimedJobsConcurrently() {
        WorkerExecutor executor = new WorkerExecutor(List.of(
            flowRecord("job-1", "queued"),
            flowRecord("job-2", "queued")
        ));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);
        Queue queue = new Queue(client, "email", "queued");
        CountDownLatch started = new CountDownLatch(2);

        QueueWorkerResult result = queue.worker("worker-1")
            .batchSize(2)
            .concurrency(2)
            .virtualThreads()
            .runOnce(job -> {
                started.countDown();
                assertTrue(started.await(2, TimeUnit.SECONDS));
                return "ok";
            });

        assertEquals(new QueueWorkerResult(2, 2, 0, 0), result);
        assertEquals(1, executor.count("FLOW.CLAIM_DUE"));
        assertEquals(2, executor.count("FLOW.COMPLETE"));
        assertEquals(2, executor.claimLimit());
    }

    @Test
    void workflowWorkerProcessesClaimedJobsConcurrently() {
        WorkerExecutor executor = new WorkerExecutor(List.of(
            flowRecord("flow-1", "created"),
            flowRecord("flow-2", "created")
        ));
        FerricStoreClient client = FerricStoreClient.fromExecutor(executor);
        CountDownLatch started = new CountDownLatch(2);
        Workflow workflow = new Workflow(client, "order", "created")
            .state("created", ctx -> {
                started.countDown();
                assertTrue(started.await(2, TimeUnit.SECONDS));
                return Outcomes.transition("charged");
            });

        int applied = workflow.worker("worker-1", List.of("created"))
            .batchSize(2)
            .concurrency(2)
            .virtualThreads()
            .runOnce();

        assertEquals(2, applied);
        assertEquals(1, executor.count("FLOW.CLAIM_DUE"));
        assertEquals(2, executor.count("FLOW.TRANSITION"));
        assertEquals(2, executor.claimLimit());
    }

    private static Object flowRecord(String id, String state) {
        return Resp.testMap(
            "id", id,
            "type", "test",
            "state", state,
            "partition_key", "p1",
            "lease_token", "lease-" + id,
            "fencing_token", 1L,
            "version", 1L
        );
    }

    private static final class WorkerExecutor implements RedisExecutor {
        private final Object claimResponse;
        private final List<List<Object>> calls = Collections.synchronizedList(new ArrayList<>());

        private WorkerExecutor(Object claimResponse) {
            this.claimResponse = claimResponse;
        }

        @Override
        public Object execute(List<Object> args) {
            calls.add(List.copyOf(args));
            if ("FLOW.CLAIM_DUE".equals(args.getFirst())) {
                return claimResponse;
            }
            return "OK";
        }

        private int count(String command) {
            synchronized (calls) {
                return (int) calls.stream().filter(call -> command.equals(call.getFirst())).count();
            }
        }

        private int claimLimit() {
            synchronized (calls) {
                List<Object> claim = calls.stream()
                    .filter(call -> "FLOW.CLAIM_DUE".equals(call.getFirst()))
                    .findFirst()
                    .orElseThrow();
                int limit = claim.indexOf("LIMIT");
                return ((Number) claim.get(limit + 1)).intValue();
            }
        }
    }
}
