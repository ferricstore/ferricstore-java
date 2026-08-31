package com.ferricstore;

import static com.ferricstore.IntegrationTestEnvironment.assumeIntegration;
import static com.ferricstore.IntegrationTestEnvironment.connectJson;
import static com.ferricstore.IntegrationTestEnvironment.suffix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Real-server coverage for non-default Flow command argument forms. */
final class FerricStoreFlowArgumentsIntegrationTest {
    @Test
    void singleLifecycleSignalMutationAndHistoryArgumentsRoundTrip() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:lifecycle:" + testId;
            String id = "java-sdk:flow-args:lifecycle:" + testId;
            String partition = id + ":partition";
            String parent = "java-sdk:flow-args:parent:" + testId;
            String root = "java-sdk:flow-args:root:" + testId;
            String correlation = "java-sdk:flow-args:correlation:" + testId;
            long now = System.currentTimeMillis();

            Object createResponse =
                    client.create(
                            CreateOptions.builder(id, type)
                                    .state("awaiting-signal")
                                    .payload(Map.of("phase", "created"))
                                    .partitionKey(partition)
                                    .parentFlowId(parent)
                                    .rootFlowId(root)
                                    .correlationId(correlation)
                                    .runAtMs(now)
                                    .nowMs(now)
                                    .priority(2)
                                    .idempotent(true)
                                    .retentionTtlMs(60_000)
                                    .maxActiveMs(45_000)
                                    .attribute("owner", "integration")
                                    .attribute("remove-me", "yes")
                                    .stateMeta("awaiting-signal", "created")
                                    .returnRecord(true)
                                    .build());
            assertTrue(createResponse instanceof FlowRecord);
            FlowRecord created = client.get(id, partition);
            assertNotNull(created);
            assertEquals(parent, created.parentFlowId());
            assertEquals(root, created.rootFlowId());
            assertEquals(correlation, created.correlationId());
            assertEquals(45_000L, created.maxActiveMs());

            assertNotNull(
                    client.signal(
                            id,
                            SignalOptions.builder("approved")
                                    .partitionKey(partition)
                                    .idempotencyKey("java-sdk:flow-args:signal:" + testId)
                                    .ifState("awaiting-signal")
                                    .ifStates(List.of("manual-review"))
                                    .transitionTo("approved")
                                    .runAtMs(now)
                                    .nowMs(now + 1)
                                    .build()));
            assertEquals("approved", client.get(id, partition).state());

            String worker = "java-sdk-flow-lifecycle-worker";
            ClaimedItem approved =
                    claimMany(client, type, "approved", partition, worker, now + 1, 1).get(0);
            assertNotNull(
                    client.transition(
                            TransitionOptions.builder(
                                            approved.id(),
                                            approved.state(),
                                            "reviewed",
                                            approved.leaseToken(),
                                            approved.fencingToken())
                                    .partitionKey(partition)
                                    .payload(Map.of("phase", "reviewed"))
                                    .runAtMs(now + 2)
                                    .nowMs(now + 2)
                                    .priority(1)
                                    .mutationFields(
                                            FlowMutationFields.builder()
                                                    .attributeMerge("stage", "reviewed")
                                                    .attributeDelete("remove-me")
                                                    .stateMeta("reviewed", "worker-transition")
                                                    .build())
                                    .build()));
            FlowRecord reviewed = client.get(id, partition);
            assertEquals("reviewed", reviewed.state());
            assertEquals("reviewed", text(reviewed.attributes().get("stage")));
            assertFalse(reviewed.attributes().containsKey("remove-me"));

            ClaimedItem finalClaim =
                    claimMany(
                                    client,
                                    type,
                                    "reviewed",
                                    partition,
                                    "java-sdk-flow-final-worker",
                                    now + 3,
                                    1)
                            .get(0);
            Object completeResponse =
                    client.complete(
                            CompleteOptions.builder(
                                            finalClaim.id(),
                                            finalClaim.leaseToken(),
                                            finalClaim.fencingToken())
                                    .partitionKey(partition)
                                    .result(Map.of("approved", true))
                                    .payload(Map.of("phase", "completed"))
                                    .ttlMs(60_000)
                                    .nowMs(now + 4)
                                    .mutationFields(
                                            FlowMutationFields.builder()
                                                    .attributeMerge("terminal", "yes")
                                                    .stateMeta("completed", "integration")
                                                    .build())
                                    .returnRecord(true)
                                    .build());
            assertTrue(completeResponse instanceof FlowRecord);
            FlowRecord completed = client.get(id, partition);
            assertEquals("completed", completed.state());
            assertEquals("yes", text(completed.attributes().get("terminal")));

            List<Object> history = client.history(id, partition, 100);
            assertFalse(history.isEmpty());
            String firstEvent = eventId(history.get(0));
            String lastEvent = eventId(history.get(history.size() - 1));
            assertNotNull(
                    client.history(
                            id,
                            HistoryOptions.builder()
                                    .partitionKey(partition)
                                    .count(100)
                                    .fromEvent(firstEvent)
                                    .toEvent(lastEvent)
                                    .fromMs(0)
                                    .toMs(now + 60_000)
                                    .fromVersion(1)
                                    .toVersion(completed.version())
                                    .reverse(true)
                                    .includeCold(true)
                                    .consistentProjection(true)
                                    .values(true)
                                    .payloadMaxBytes(65_536)
                                    .build()));
            assertNotNull(
                    client.history(
                            id,
                            HistoryOptions.builder()
                                    .partitionKey(partition)
                                    .count(20)
                                    .event(eventName(history.get(0)))
                                    .build()));
            assertNotNull(
                    client.history(
                            id,
                            HistoryOptions.builder()
                                    .partitionKey(partition)
                                    .count(20)
                                    .worker(worker)
                                    .build()));
        }
    }

    @Test
    void createManyAndClaimArgumentFormsRoundTrip() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:create:" + testId;
            long now = System.currentTimeMillis();

            String automaticId = "java-sdk:flow-args:auto:" + testId;
            assertNotNull(
                    client.createMany(
                            CreateManyOptions.builder(
                                            type,
                                            List.of(
                                                    new CreateItem(
                                                            automaticId, Map.of("mode", "auto"))))
                                    .state("auto")
                                    .runAtMs(now)
                                    .nowMs(now)
                                    .priority(2)
                                    .idempotent(true)
                                    .independent(true)
                                    .retentionTtlMs(60_000)
                                    .attribute("suite", "flow-arguments")
                                    .stateMeta("auto", "attempt-1")
                                    .build()));
            FlowRecord automatic = client.get(automaticId, null);
            assertNotNull(automatic);
            assertEquals("auto", automatic.state());
            assertEquals("flow-arguments", text(automatic.attributes().get("suite")));

            String mixedIdA = "java-sdk:flow-args:mixed:" + testId + ":a";
            String mixedIdB = "java-sdk:flow-args:mixed:" + testId + ":b";
            String mixedPartitionA = mixedIdA + ":partition";
            String mixedPartitionB = mixedIdB + ":partition";
            assertNotNull(
                    client.createMany(
                            CreateManyOptions.builder(
                                            type,
                                            List.of(
                                                    new CreateItem(
                                                            mixedIdA,
                                                            Map.of("mode", "mixed-a"),
                                                            mixedPartitionA),
                                                    new CreateItem(
                                                            mixedIdB,
                                                            Map.of("mode", "mixed-b"),
                                                            mixedPartitionB)))
                                    .state("mixed")
                                    .runAtMs(now)
                                    .nowMs(now)
                                    .build()));
            assertNotNull(client.get(mixedIdA, mixedPartitionA));
            assertNotNull(client.get(mixedIdB, mixedPartitionB));

            String mappedPartition = "java-sdk:flow-args:mapped:" + testId + ":partition";
            String mappedId = "java-sdk:flow-args:mapped:" + testId;
            assertNotNull(
                    client.createMany(
                            CreateManyOptions.builder(
                                            type,
                                            List.of(
                                                    new CreateItem(
                                                            mappedId, Map.of("mode", "mapped"))))
                                    .partitionKey(mappedPartition)
                                    .state("mapped")
                                    .runAtMs(now)
                                    .nowMs(now)
                                    .maxActiveMs(30_000)
                                    .build()));
            assertEquals(30_000L, client.get(mappedId, mappedPartition).maxActiveMs());

            String payloadPartition = "java-sdk:flow-args:payload:" + testId + ":partition";
            String payloadId = "java-sdk:flow-args:payload:" + testId;
            create(client, payloadId, type, "payload", payloadPartition, now);
            List<FlowRecord> hydratedClaims =
                    client.claimDue(
                            ClaimDueOptions.builder(type, "java-sdk-flow-payload-worker")
                                    .state("payload")
                                    .partitionKey(payloadPartition)
                                    .leaseMs(30_000)
                                    .limit(1)
                                    .nowMs(now)
                                    .payload(true)
                                    .payloadMaxBytes(65_536)
                                    .build());
            assertEquals(1, hydratedClaims.size());
            assertEquals(Map.of("id", payloadId), hydratedClaims.get(0).payload());

            List<ClaimedItem> mixedClaims =
                    client.claimJobs(
                            ClaimDueOptions.builder(type, "java-sdk-flow-argument-worker")
                                    .states(List.of("mixed", "mapped"))
                                    .partitionKeys(
                                            List.of(
                                                    mixedPartitionA,
                                                    mixedPartitionB,
                                                    mappedPartition))
                                    .leaseMs(30_000)
                                    // The OSS router divides this budget among participating
                                    // shards without backfilling unused per-shard quota.
                                    .limit(100)
                                    .nowMs(now)
                                    .blockMs(1)
                                    .payload(true)
                                    .payloadMaxBytes(65_536)
                                    .includeState(true)
                                    .includeAttributes(true)
                                    .build());
            assertEquals(3, mixedClaims.size());
            assertTrue(
                    mixedClaims.stream()
                            .allMatch(
                                    item -> List.of("mixed", "mapped").contains(item.runState())));
            assertTrue(
                    client.claimJobs(
                                    ClaimDueOptions.builder(
                                                    type, "java-sdk-flow-reclaim-option-worker")
                                            .states(List.of("mixed", "mapped"))
                                            .partitionKeys(
                                                    List.of(
                                                            mixedPartitionA,
                                                            mixedPartitionB,
                                                            mappedPartition))
                                            .limit(1)
                                            .nowMs(now)
                                            .reclaimExpired(true)
                                            .reclaimRatio(2)
                                            .build())
                            .isEmpty());
        }
    }

    @Test
    void completeManyAndCancelManyApplyRichSameAndMixedPartitionArguments() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:many:" + testId;
            long now = System.currentTimeMillis();

            String completePartition = "java-sdk:flow-args:complete-many:" + testId;
            List<String> completeIds = List.of(completePartition + ":a", completePartition + ":b");
            client.createMany(
                    CreateManyOptions.builder(
                                    type,
                                    completeIds.stream()
                                            .map(
                                                    id ->
                                                            new CreateItem(
                                                                    id, Map.of("phase", "created")))
                                            .toList())
                            .partitionKey(completePartition)
                            .state("complete-many")
                            .runAtMs(now)
                            .nowMs(now)
                            .attribute("remove-me", true)
                            .build());
            List<ClaimedItem> completeClaims =
                    claimMany(
                            client,
                            type,
                            "complete-many",
                            completePartition,
                            "java-sdk-complete-many-worker",
                            now,
                            2);
            Object completed =
                    client.completeMany(
                            CompleteManyOptions.builder(completeClaims)
                                    .partitionKey(completePartition)
                                    .result(Map.of("ok", true))
                                    .payload(Map.of("phase", "completed"))
                                    .ttlMs(60_000)
                                    .nowMs(now + 1)
                                    .independent(true)
                                    .value("batch-value", Map.of("mode", "complete"))
                                    .mutationFields(
                                            FlowMutationFields.builder()
                                                    .attributeMerge("stage", "done")
                                                    .attributeDelete("remove-me")
                                                    .stateMeta("completed", "batch")
                                                    .build())
                                    .returnOkOnSuccess(true)
                                    .build());
            assertTrue(ok(completed));
            for (String id : completeIds) {
                FlowRecord record = client.get(id, completePartition);
                assertEquals("completed", record.state());
                assertNotNull(record.raw().get("payload_ref"));
                assertNotNull(record.raw().get("result_ref"));
                assertEquals("done", text(record.attributes().get("stage")));
                assertFalse(record.attributes().containsKey("remove-me"));
                assertEquals(
                        Map.of("mode", "complete"),
                        hydratedGet(client, id, completePartition, "batch-value")
                                .values()
                                .get("batch-value"));
            }

            String failPartition = "java-sdk:flow-args:fail-many:" + testId;
            List<String> failIds = List.of(failPartition + ":a", failPartition + ":b");
            client.createMany(
                    CreateManyOptions.builder(
                                    type,
                                    failIds.stream()
                                            .map(
                                                    id ->
                                                            new CreateItem(
                                                                    id, Map.of("phase", "created")))
                                            .toList())
                            .partitionKey(failPartition)
                            .state("fail-many")
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            List<ClaimedItem> failClaims =
                    claimMany(
                            client,
                            type,
                            "fail-many",
                            failPartition,
                            "java-sdk-fail-many-worker",
                            now,
                            2);
            assertNotNull(
                    client.failMany(
                            FailManyOptions.builder(failClaims)
                                    .partitionKey(failPartition)
                                    .error(Map.of("reason", "integration"))
                                    .payload(Map.of("phase", "failed"))
                                    .ttlMs(60_000)
                                    .nowMs(now + 2)
                                    .independent(true)
                                    .value("failure-detail", Map.of("retryable", false))
                                    .mutationFields(
                                            FlowMutationFields.builder()
                                                    .attributeMerge("failed-by", "integration")
                                                    .stateMeta("failed", "batch")
                                                    .build())
                                    .build()));
            for (String failId : failIds) {
                FlowRecord failed = hydratedGet(client, failId, failPartition, "failure-detail");
                assertEquals("failed", failed.state());
                assertEquals(Map.of("retryable", false), failed.values().get("failure-detail"));
                assertEquals("integration", text(failed.attributes().get("failed-by")));
            }

            String transitionPartition = "java-sdk:flow-args:transition-many:" + testId;
            List<String> transitionIds =
                    List.of(transitionPartition + ":a", transitionPartition + ":b");
            client.createMany(
                    CreateManyOptions.builder(
                                    type,
                                    transitionIds.stream()
                                            .map(
                                                    id ->
                                                            new CreateItem(
                                                                    id, Map.of("phase", "created")))
                                            .toList())
                            .partitionKey(transitionPartition)
                            .state("transition-many")
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            List<ClaimedItem> transitionClaims =
                    claimMany(
                            client,
                            type,
                            "transition-many",
                            transitionPartition,
                            "java-sdk-transition-many-worker",
                            now,
                            2);
            Object transitioned =
                    client.transitionMany(
                            TransitionManyOptions.builder(
                                            transitionClaims.get(0).state(),
                                            "transitioned-many",
                                            transitionClaims.stream()
                                                    .map(
                                                            claim ->
                                                                    new FencedItem(
                                                                            claim.id(),
                                                                            claim.fencingToken(),
                                                                            claim.leaseToken(),
                                                                            transitionPartition))
                                                    .toList())
                                    .partitionKey(transitionPartition)
                                    .payload(Map.of("phase", "transitioned"))
                                    .runAtMs(now + 3)
                                    .nowMs(now + 3)
                                    .priority(1)
                                    .independent(true)
                                    .value("transition-detail", Map.of("mode", "batch"))
                                    .returnOkOnSuccess(true)
                                    .build());
            assertTrue(ok(transitioned));
            for (String transitionId : transitionIds) {
                FlowRecord transitionedRecord =
                        hydratedGet(client, transitionId, transitionPartition, "transition-detail");
                assertEquals("transitioned-many", transitionedRecord.state());
                assertEquals(
                        Map.of("mode", "batch"),
                        transitionedRecord.values().get("transition-detail"));
            }

            String cancelIdA = "java-sdk:flow-args:cancel-many:" + testId + ":a";
            String cancelIdB = "java-sdk:flow-args:cancel-many:" + testId + ":b";
            String cancelPartitionA = cancelIdA + ":partition";
            String cancelPartitionB = cancelIdB + ":partition";
            create(client, cancelIdA, type, "cancel-many", cancelPartitionA, now);
            create(client, cancelIdB, type, "cancel-many", cancelPartitionB, now);
            FlowRecord cancelA = client.get(cancelIdA, cancelPartitionA);
            FlowRecord cancelB = client.get(cancelIdB, cancelPartitionB);
            assertNotNull(
                    client.cancelMany(
                            CancelManyOptions.builder(
                                            List.of(
                                                    new FencedItem(
                                                            cancelA.id(),
                                                            cancelA.fencingToken(),
                                                            cancelPartitionA),
                                                    new FencedItem(
                                                            cancelB.id(),
                                                            cancelB.fencingToken(),
                                                            cancelPartitionB)))
                                    .reason(Map.of("reason", "operator"))
                                    .ttlMs(60_000)
                                    .nowMs(now + 2)
                                    .independent(true)
                                    .value("cancel-detail", Map.of("mode", "batch"))
                                    .mutationFields(
                                            FlowMutationFields.builder()
                                                    .attributeMerge("cancelled-by", "integration")
                                                    .stateMeta("cancelled", "forced")
                                                    .build())
                                    .build()));
            for (Map.Entry<String, String> item :
                    Map.of(cancelIdA, cancelPartitionA, cancelIdB, cancelPartitionB).entrySet()) {
                FlowRecord record = client.get(item.getKey(), item.getValue());
                assertEquals("cancelled", record.state());
                assertEquals("integration", text(record.attributes().get("cancelled-by")));
                assertEquals(
                        Map.of("mode", "batch"),
                        hydratedGet(client, item.getKey(), item.getValue(), "cancel-detail")
                                .values()
                                .get("cancel-detail"));
            }
        }
    }

    @Test
    void namedValuesAndExtendedItemArgumentsRoundTrip() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:named:" + testId;
            long now = System.currentTimeMillis();
            String id = "java-sdk:flow-args:named:" + testId;
            String partition = id + ":partition";

            Map<String, Object> shared =
                    client.valuePut(Map.of("scope", "shared"), null, null, partition, 60_000L);
            String sharedRef = text(shared.get("ref"));
            assertNotNull(sharedRef);

            client.create(
                    CreateOptions.builder(id, type)
                            .state("named")
                            .partitionKey(partition)
                            .runAtMs(now)
                            .nowMs(now)
                            .value("document", Map.of("version", 1))
                            .valueRef("shared", sharedRef)
                            .build());
            FlowRecord created = hydratedGet(client, id, partition, "document", "shared");
            assertEquals(Map.of("version", 1), created.values().get("document"));
            assertTrue(created.valueRefs().get("shared") instanceof Map<?, ?>);
            assertEquals(
                    sharedRef, text(((Map<?, ?>) created.valueRefs().get("shared")).get("ref")));
            assertEquals(List.of(Map.of("scope", "shared")), client.valueMGet(List.of(sharedRef)));

            Map<String, Object> firstOwned =
                    client.valuePut(Map.of("version", 1), "owned", id, partition, null, false);
            Map<String, Object> replacedOwned =
                    client.valuePut(Map.of("version", 2), "owned", id, partition, null, true);
            assertNotEquals(text(firstOwned.get("ref")), text(replacedOwned.get("ref")));
            assertEquals(
                    Map.of("version", 2),
                    hydratedGet(client, id, partition, "owned").values().get("owned"));

            ClaimedItem claimed =
                    claimMany(client, type, "named", partition, "java-sdk-named-worker", now, 1)
                            .get(0);
            client.transition(
                    TransitionOptions.builder(
                                    claimed.id(),
                                    claimed.state(),
                                    "named-transitioned",
                                    claimed.leaseToken(),
                                    claimed.fencingToken())
                            .partitionKey(partition)
                            .nowMs(now + 1)
                            .runAtMs(now + 1)
                            .value("document", Map.of("version", 2))
                            .mutationFields(
                                    FlowMutationFields.builder().overrideValue("document").build())
                            .build());
            assertEquals(
                    Map.of("version", 2),
                    hydratedGet(client, id, partition, "document").values().get("document"));

            ClaimedItem completing =
                    claimMany(
                                    client,
                                    type,
                                    "named-transitioned",
                                    partition,
                                    "java-sdk-named-complete-worker",
                                    now + 1,
                                    1)
                            .get(0);
            client.complete(
                    CompleteOptions.builder(
                                    completing.id(),
                                    completing.leaseToken(),
                                    completing.fencingToken())
                            .partitionKey(partition)
                            .nowMs(now + 2)
                            .value("result-detail", Map.of("ok", true))
                            .mutationFields(
                                    FlowMutationFields.builder().dropValue("document").build())
                            .build());
            assertFalse(
                    hydratedGet(client, id, partition, "document")
                            .values()
                            .containsKey("document"));
            assertEquals(
                    Map.of("ok", true),
                    hydratedGet(client, id, partition, "result-detail")
                            .values()
                            .get("result-detail"));

            String extendedPartition = "java-sdk:flow-args:extended:" + testId;
            String extendedId = extendedPartition + ":item";
            client.createMany(
                    CreateManyOptions.builder(
                                    type,
                                    List.of(
                                            new CreateItem(
                                                    extendedId,
                                                    Map.of("shape", "extended"),
                                                    null,
                                                    Map.of(
                                                            "item-value",
                                                            Map.of("kind", "extended")),
                                                    Map.of())))
                            .partitionKey(extendedPartition)
                            .state("extended")
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            assertEquals(
                    Map.of("kind", "extended"),
                    hydratedGet(client, extendedId, extendedPartition, "item-value")
                            .values()
                            .get("item-value"));

            String mappedPartition = "java-sdk:flow-args:mapped-values:" + testId;
            String mappedId = mappedPartition + ":item";
            client.createMany(
                    CreateManyOptions.builder(
                                    type,
                                    List.of(
                                            new CreateItem(
                                                    mappedId,
                                                    Map.of("shape", "mapped"),
                                                    null,
                                                    Map.of("item-value", Map.of("kind", "mapped")),
                                                    Map.of(),
                                                    MaxActiveMs.of(30_000))))
                            .partitionKey(mappedPartition)
                            .state("mapped-values")
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            FlowRecord mapped = hydratedGet(client, mappedId, mappedPartition, "item-value");
            assertEquals(30_000L, mapped.maxActiveMs());
            assertEquals(Map.of("kind", "mapped"), mapped.values().get("item-value"));

            String parentId = "java-sdk:flow-args:named-parent:" + testId;
            String parentPartition = parentId + ":partition";
            client.create(
                    CreateOptions.builder(parentId, type)
                            .state("dispatch")
                            .partitionKey(parentPartition)
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            FlowRecord parent = client.get(parentId, parentPartition);
            String childId = "java-sdk:flow-args:named-child:" + testId;
            client.spawnChildren(
                    SpawnChildrenOptions.builder(
                                    parentId,
                                    List.of(
                                            new ChildSpec(
                                                    childId,
                                                    type,
                                                    Map.of("child", true),
                                                    null,
                                                    Map.of(
                                                            "child-value",
                                                            Map.of("source", "parent")),
                                                    Map.of(),
                                                    MaxActiveMs.of(20_000))))
                            .partitionKey(parentPartition)
                            .fencingToken(parent.fencingToken())
                            .groupId("named-values")
                            .waitMode("all")
                            .fromState("dispatch")
                            .waitState("waiting")
                            .success("children-complete")
                            .failure("children-failed")
                            .nowMs(now + 3)
                            .build());
            FlowRecord child = hydratedGet(client, childId, parentPartition, "child-value");
            assertEquals(20_000L, child.maxActiveMs());
            assertEquals(Map.of("source", "parent"), child.values().get("child-value"));
        }
    }

    @Test
    void reservedMarkerChildIdRoundTripsWithoutGrammarAmbiguity() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:mixed-children:" + testId;
            String parentId = type + ":parent";
            String parentPartition = parentId + ":partition";
            String childA = "MIXED";
            String childB = type + ":b";
            long now = System.currentTimeMillis();

            client.create(
                    CreateOptions.builder(parentId, type)
                            .state("dispatch")
                            .partitionKey(parentPartition)
                            .runAtMs(now)
                            .nowMs(now)
                            .build());
            FlowRecord parent = client.get(parentId, parentPartition);
            client.spawnChildren(
                    SpawnChildrenOptions.builder(
                                    parentId,
                                    List.of(
                                            new ChildSpec(childA, type, Map.of("child", "a")),
                                            new ChildSpec(childB, type, Map.of("child", "b"))))
                            .partitionKey(parentPartition)
                            .fencingToken(parent.fencingToken())
                            .groupId("mixed-partitions")
                            .waitMode("all")
                            .fromState("dispatch")
                            .waitState("waiting")
                            .success("children-complete")
                            .failure("children-failed")
                            .nowMs(now + 1)
                            .build());

            assertEquals(
                    Map.of("child", "a"), hydratedGet(client, childA, parentPartition).payload());
            assertEquals(
                    Map.of("child", "b"), hydratedGet(client, childB, parentPartition).payload());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void stepAndRecurringScheduleArgumentFormsRoundTrip() {
        assumeIntegration();

        try (FerricStoreClient client = connectJson()) {
            String testId = suffix();
            String type = "java-sdk:flow-args:steps:" + testId;
            String partition = "java-sdk:flow-args:steps:" + testId + ":partition";
            String id = "java-sdk:flow-args:step:" + testId;
            long now = System.currentTimeMillis();

            FlowRecord started =
                    client.startAndClaim(
                            StartAndClaimOptions.builder(
                                            id, type, "step-one", "java-sdk-flow-step-worker")
                                    .leaseMs(20_000)
                                    .payload(Map.of("step", 1))
                                    .partitionKey(partition)
                                    .parentFlowId("java-sdk:flow-args:step-parent:" + testId)
                                    .rootFlowId("java-sdk:flow-args:step-root:" + testId)
                                    .correlationId("java-sdk:flow-args:step-correlation:" + testId)
                                    .nowMs(now)
                                    .priority(1)
                                    .retentionTtlMs(60_000)
                                    .maxActiveMs(30_000)
                                    .attribute("stage", "one")
                                    .attribute("remove-me", "yes")
                                    .stateMeta("step-one", "started")
                                    .build());
            assertNotNull(started);

            ClaimedItem continued =
                    stage(
                            "FLOW.STEP_CONTINUE rich arguments",
                            () ->
                                    (ClaimedItem)
                                            client.flowSteps()
                                                    .continueStep(
                                                            FlowSteps.ContinueOptions.builder(
                                                                            started.id(),
                                                                            started.leaseToken(),
                                                                            started.fencingToken(),
                                                                            "step-one",
                                                                            "step-two")
                                                                    .leaseMs(25_000)
                                                                    .partitionKey(partition)
                                                                    .payload(Map.of("step", 2))
                                                                    .attribute("stage", "two")
                                                                    .deleteAttribute("remove-me")
                                                                    .stateMeta(
                                                                            "step-two", "continued")
                                                                    .nowMs(now + 1)
                                                                    .worker(
                                                                            "java-sdk-flow-step-worker")
                                                                    .returnJob(true)
                                                                    .build()));
            assertEquals(id, continued.id());
            assertEquals("step-two", text(client.get(id, partition).raw().get("run_state")));
            assertNotNull(
                    stage(
                            "FLOW.COMPLETE after STEP_CONTINUE",
                            () ->
                                    client.complete(
                                            CompleteOptions.builder(
                                                            continued.id(),
                                                            continued.leaseToken(),
                                                            continued.fencingToken())
                                                    .partitionKey(partition)
                                                    .result(Map.of("done", true))
                                                    .nowMs(now + 2)
                                                    .build())));

            String runManyId = "java-sdk:flow-args:run-many:" + testId;
            assertNotNull(
                    client.flowSteps()
                            .runMany(
                                    FlowSteps.RunManyOptions.builder(
                                                    type,
                                                    List.of(
                                                            new FlowSteps.RunItem(
                                                                    runManyId, partition)))
                                            .steps(3)
                                            .worker("java-sdk-flow-run-many-worker")
                                            .leaseMs(15_000)
                                            .nowMs(now + 3)
                                            .payload(Map.of("input", true))
                                            .result(Map.of("output", true))
                                            .retentionTtlMs(60_000)
                                            .build()));
            assertNotNull(client.get(runManyId, partition));

            FlowSchedules schedules = client.flowSchedules();
            String scheduleId = "java-sdk:flow-args:interval:" + testId;
            Map<String, Object> target =
                    Map.of(
                            "id_prefix",
                            "java-sdk:flow-args:scheduled:" + testId,
                            "type",
                            type,
                            "state",
                            "scheduled",
                            "partition_key",
                            partition,
                            "payload",
                            Map.of("scheduled", true));
            assertNotNull(
                    schedules.create(
                            scheduleId,
                            FlowSchedules.CreateOptions.builder(target)
                                    .kind("interval")
                                    .startAtMs(now + 60_000)
                                    .everyMs(60_000)
                                    .catchupPolicy("fire_once")
                                    .overlapPolicy("queue_after_previous")
                                    .overlapRetryMs(1_000)
                                    .maxFires(3)
                                    .endAtMs(now + 360_000)
                                    .overwrite(true)
                                    .nowMs(now)
                                    .build()));
            assertNotNull(schedules.get(scheduleId));
            List<Map<String, Object>> listed =
                    schedules.list(
                            FlowSchedules.ListOptions.builder()
                                    .kind("interval")
                                    .state("active")
                                    .targetType(type)
                                    .fromMs(now)
                                    .toMs(now + 360_000)
                                    .count(20)
                                    .reverse(true)
                                    .build());
            assertTrue(listed.stream().anyMatch(item -> scheduleId.equals(text(item.get("id")))));
            assertNotNull(schedules.pause(scheduleId, now + 4));
            assertNotNull(schedules.resume(scheduleId, now + 5));
            schedules.delete(scheduleId, now + 6);

            String cronScheduleId = "java-sdk:flow-args:cron:" + testId;
            Map<String, Object> cronTarget =
                    Map.of(
                            "id_prefix",
                            "java-sdk:flow-args:cron-target:" + testId,
                            "type",
                            type,
                            "state",
                            "cron-scheduled",
                            "partition_key",
                            partition);
            assertNotNull(
                    schedules.create(
                            cronScheduleId,
                            FlowSchedules.CreateOptions.builder(cronTarget)
                                    .kind("cron")
                                    .cron("*/5 * * * *")
                                    .timezone("UTC")
                                    .overlapPolicy("skip")
                                    .maxFires(2)
                                    .endAtMs(now + 360_000)
                                    .overwrite(true)
                                    .nowMs(now)
                                    .build()));
            assertTrue(
                    schedules
                            .list(
                                    FlowSchedules.ListOptions.builder()
                                            .kind("cron")
                                            .state("active")
                                            .timezone("UTC")
                                            .targetType(type)
                                            .count(20)
                                            .build())
                            .stream()
                            .anyMatch(item -> cronScheduleId.equals(text(item.get("id")))));
            schedules.delete(cronScheduleId, now + 7);

            String delayScheduleId = "java-sdk:flow-args:delay:" + testId;
            Map<String, Object> delayTarget =
                    Map.of(
                            "id",
                            "java-sdk:flow-args:delay-target:" + testId,
                            "type",
                            type,
                            "state",
                            "delay-scheduled",
                            "partition_key",
                            partition);
            assertNotNull(
                    schedules.create(
                            delayScheduleId,
                            FlowSchedules.CreateOptions.builder(delayTarget)
                                    .kind("delay")
                                    .delayMs(60_000)
                                    .overwrite(true)
                                    .nowMs(now)
                                    .build()));
            assertNotNull(schedules.get(delayScheduleId));
            schedules.delete(delayScheduleId, now + 8);
        }
    }

    private static void create(
            FerricStoreClient client,
            String id,
            String type,
            String state,
            String partition,
            long now) {
        assertNotNull(
                client.create(
                        CreateOptions.builder(id, type)
                                .state(state)
                                .partitionKey(partition)
                                .payload(Map.of("id", id))
                                .runAtMs(now)
                                .nowMs(now)
                                .build()));
    }

    private static List<ClaimedItem> claimMany(
            FerricStoreClient client,
            String type,
            String state,
            String partition,
            String worker,
            long now,
            int limit) {
        List<ClaimedItem> claimed =
                client.claimJobs(
                        ClaimDueOptions.builder(type, worker)
                                .state(state)
                                .partitionKey(partition)
                                .leaseMs(30_000)
                                .limit(limit)
                                .nowMs(now)
                                .payload(true)
                                .includeState(true)
                                .includeAttributes(true)
                                .build());
        assertEquals(limit, claimed.size());
        return claimed;
    }

    private static String text(Object value) {
        assertNotNull(value);
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    private static FlowRecord hydratedGet(
            FerricStoreClient client, String id, String partition, String... valueNames) {
        List<Object> command =
                new java.util.ArrayList<>(
                        List.of("FLOW.GET", id, "PARTITION", partition, "FULL", "true"));
        for (String valueName : valueNames) {
            command.add("VALUE");
            command.add(valueName);
        }
        return Resp.optionalRecord(client.command(command), client.codec());
    }

    private static boolean ok(Object value) {
        return "OK".equalsIgnoreCase(text(value));
    }

    private static String eventId(Object event) {
        if (event instanceof List<?> list && !list.isEmpty()) {
            return text(list.get(0));
        }
        if (event instanceof Map<?, ?> map) {
            Object value = map.get("event_id");
            if (value == null) {
                value = map.get("id");
            }
            return text(value);
        }
        throw new AssertionError("history event did not expose an event id");
    }

    private static String eventName(Object event) {
        Object fields = event;
        if (event instanceof List<?> list && list.size() > 1) {
            fields = list.get(1);
        }
        Map<String, Object> mapped = Resp.map(fields);
        Object name = mapped.get("event");
        if (name == null && mapped.get("fields") != null) {
            name = Resp.map(mapped.get("fields")).get("event");
        }
        if (name == null) {
            throw new AssertionError("history event did not expose an event name: " + event);
        }
        return text(name);
    }

    private static <T> T stage(String name, Supplier<T> operation) {
        try {
            return operation.get();
        } catch (RuntimeException error) {
            throw new AssertionError(name + " failed", error);
        }
    }
}
