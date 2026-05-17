package com.ferricstore;

import java.util.ArrayList;
import java.util.List;

public final class FerricStoreClient implements AutoCloseable {
    private final RedisExecutor executor;
    private final AutoCloseable closeable;

    private FerricStoreClient(RedisExecutor executor, AutoCloseable closeable) {
        this.executor = executor;
        this.closeable = closeable;
    }

    public static FerricStoreClient connect(String redisUri) {
        JedisRedisExecutor executor = JedisRedisExecutor.connect(redisUri);
        return new FerricStoreClient(executor, executor);
    }

    public static FerricStoreClient fromExecutor(RedisExecutor executor) {
        return new FerricStoreClient(executor, null);
    }

    public Object execute(List<Object> args) {
        return executor.execute(List.copyOf(args));
    }

    public void create(CreateOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> args = args("FLOW.CREATE", options.id(), "TYPE", options.type(), "STATE", defaultState(options.state()), "NOW", now);
        appendOpt(args, "PARTITION", options.partitionKey());
        appendOpt(args, "PAYLOAD", options.payload());
        appendOpt(args, "PARENT_FLOW_ID", options.parentFlowId());
        appendOpt(args, "ROOT_FLOW_ID", options.rootFlowId());
        appendOpt(args, "CORRELATION_ID", options.correlationId());
        appendOpt(args, "RUN_AT", runAt);
        appendOpt(args, "PRIORITY", options.priority());
        appendOpt(args, "IDEMPOTENT", options.idempotent());
        if (options.returnRecord()) {
            args.add("RETURN");
            args.add("RECORD");
        }
        executor.execute(args);
    }

    public void createMany(CreateManyOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        boolean mixed = options.partitionKey() == null || options.partitionKey().isEmpty();
        String wirePartition = mixed ? "MIXED" : options.partitionKey();
        List<Object> args = args("FLOW.CREATE_MANY", wirePartition, "TYPE", options.type(), "STATE", defaultState(options.state()), "NOW", now);
        appendOpt(args, "RUN_AT", runAt);
        appendOpt(args, "PRIORITY", options.priority());
        appendOpt(args, "IDEMPOTENT", options.idempotent());
        appendOpt(args, "INDEPENDENT", options.independent());
        args.add("ITEMS");
        for (CreateItem item : options.items()) {
            if (mixed) {
                if (item.partitionKey() == null || item.partitionKey().isEmpty()) {
                    throw new IllegalArgumentException("mixed createMany items require partition key");
                }
                args.add(item.id());
                args.add(item.partitionKey());
                args.add(item.payload());
            } else {
                args.add(item.id());
                args.add(item.payload());
            }
        }
        executor.execute(args);
    }

    public List<FlowRecord> claimDue(ClaimDueOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        int limit = options.limit() == 0 ? 1 : options.limit();
        long leaseMs = options.leaseMs() == 0 ? 30_000 : options.leaseMs();
        List<Object> args = args(
            "FLOW.CLAIM_DUE", options.type(),
            "STATE", defaultState(options.state()),
            "WORKER", options.worker(),
            "LEASE", leaseMs,
            "LIMIT", limit,
            "NOW", now
        );
        appendOpt(args, "PARTITION", options.partitionKey());
        appendOpt(args, "RECLAIM_EXPIRED", options.reclaimExpired());
        appendOpt(args, "RECLAIM_RATIO", options.reclaimRatio());
        return Resp.records(executor.execute(args));
    }

    public void complete(CompleteOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        List<Object> args = args("FLOW.COMPLETE", options.id(), options.leaseToken(), "FENCING", options.fencingToken(), "NOW", now);
        appendOpt(args, "PARTITION", options.partitionKey());
        appendOpt(args, "RESULT", options.result());
        appendOpt(args, "PAYLOAD", options.payload());
        appendOpt(args, "TTL", options.ttlMs());
        if (options.returnRecord()) {
            args.add("RETURN");
            args.add("RECORD");
        }
        executor.execute(args);
    }

    public void transition(TransitionOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> args = args(
            "FLOW.TRANSITION", options.id(), options.fromState(), options.toState(),
            options.leaseToken(), "FENCING", options.fencingToken(), "NOW", now
        );
        appendOpt(args, "PARTITION", options.partitionKey());
        appendOpt(args, "PAYLOAD", options.payload());
        appendOpt(args, "RUN_AT", runAt);
        appendOpt(args, "PRIORITY", options.priority());
        if (options.returnRecord()) {
            args.add("RETURN");
            args.add("RECORD");
        }
        executor.execute(args);
    }

    public void completeMany(CompleteManyOptions options) {
        boolean mixed = options.partitionKey() == null || options.partitionKey().isEmpty();
        String wirePartition = mixed ? "MIXED" : options.partitionKey();
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        List<Object> args = args("FLOW.COMPLETE_MANY", wirePartition);
        appendOpt(args, "RESULT", options.result());
        appendOpt(args, "PAYLOAD", options.payload());
        appendOpt(args, "TTL", options.ttlMs());
        appendOpt(args, "NOW", now);
        appendOpt(args, "INDEPENDENT", options.independent());
        args.add("ITEMS");
        for (ClaimedItem item : options.items()) {
            if (mixed) {
                if (item.partitionKey() == null || item.partitionKey().isEmpty()) {
                    throw new IllegalArgumentException("mixed completeMany items require partition key");
                }
                args.add(item.id());
                args.add(item.partitionKey());
                args.add(item.leaseToken());
                args.add(item.fencingToken());
            } else {
                args.add(item.id());
                args.add(item.leaseToken());
                args.add(item.fencingToken());
            }
        }
        executor.execute(args);
    }

    public void incr(String key) {
        executor.execute(args("INCR", key));
    }

    @Override
    public void close() {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            throw new FerricStoreException("failed to close FerricStore client", e);
        }
    }

    private static long nowMs() {
        return System.currentTimeMillis();
    }

    private static String defaultState(String state) {
        return state == null || state.isEmpty() ? "queued" : state;
    }

    private static List<Object> args(Object... values) {
        return new ArrayList<>(List.of(values));
    }

    private static void appendOpt(List<Object> args, String name, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String text && text.isEmpty()) {
            return;
        }
        args.add(name);
        args.add(value);
    }
}
