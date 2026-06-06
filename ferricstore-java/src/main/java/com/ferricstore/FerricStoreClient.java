package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendBool;
import static com.ferricstore.CommandArgs.appendEncoded;
import static com.ferricstore.CommandArgs.appendNamedValues;
import static com.ferricstore.CommandArgs.appendPayloadRead;
import static com.ferricstore.CommandArgs.args;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FerricStoreClient implements AutoCloseable {
    private final RedisExecutor executor;
    private final AutoCloseable closeable;
    private final Codec codec;
    private final KeyValueStore kv;
    private final HashStore hash;
    private final ListStore lists;
    private final SetStore sets;
    private final SortedSetStore zset;
    private final StreamStore stream;
    private final BitmapStore bitmap;
    private final HyperLogLogStore hyperloglog;
    private final GeoStore geo;
    private final JsonStore json;
    private final BloomFilterStore bloom;
    private final CuckooFilterStore cuckoo;
    private final CountMinSketchStore cms;
    private final TopKStore topk;
    private final TDigestStore tdigest;

    private FerricStoreClient(RedisExecutor executor, AutoCloseable closeable, Codec codec) {
        this.executor = executor;
        this.closeable = closeable;
        this.codec = codec == null ? new RawCodec() : codec;
        this.kv = new KeyValueStore(this);
        this.hash = new HashStore(this);
        this.lists = new ListStore(this);
        this.sets = new SetStore(this);
        this.zset = new SortedSetStore(this);
        this.stream = new StreamStore(this);
        this.bitmap = new BitmapStore(this);
        this.hyperloglog = new HyperLogLogStore(this);
        this.geo = new GeoStore(this);
        this.json = new JsonStore(this);
        this.bloom = new BloomFilterStore(this);
        this.cuckoo = new CuckooFilterStore(this);
        this.cms = new CountMinSketchStore(this);
        this.topk = new TopKStore(this);
        this.tdigest = new TDigestStore(this);
    }

    public static FerricStoreClient connect(String redisUri) {
        return connect(redisUri, new RawCodec());
    }

    public static FerricStoreClient connect(String redisUri, Codec codec) {
        JedisRedisExecutor executor = JedisRedisExecutor.connect(redisUri);
        return new FerricStoreClient(executor, executor, codec);
    }

    public static FerricStoreClient fromExecutor(RedisExecutor executor) {
        return fromExecutor(executor, new RawCodec());
    }

    public static FerricStoreClient fromExecutor(RedisExecutor executor, Codec codec) {
        return new FerricStoreClient(executor, null, codec);
    }

    public Codec codec() {
        return codec;
    }

    public Object command(Object... args) {
        return command(List.of(args));
    }

    public Object command(List<Object> args) {
        return executor.execute(List.copyOf(args));
    }

    public List<Object> pipeline(List<List<Object>> commands) {
        return executor.pipeline(commands.stream().map(List::copyOf).toList());
    }

    public KeyValueStore kv() { return kv; }
    public HashStore hash() { return hash; }
    public ListStore lists() { return lists; }
    public SetStore sets() { return sets; }
    public SortedSetStore zset() { return zset; }
    public StreamStore stream() { return stream; }
    public BitmapStore bitmap() { return bitmap; }
    public HyperLogLogStore hyperloglog() { return hyperloglog; }
    public GeoStore geo() { return geo; }
    public JsonStore json() { return json; }
    public BloomFilterStore bloom() { return bloom; }
    public CuckooFilterStore cuckoo() { return cuckoo; }
    public CountMinSketchStore cms() { return cms; }
    public TopKStore topk() { return topk; }
    public TDigestStore tdigest() { return tdigest; }

    public Object create(CreateOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> cmd = args("FLOW.CREATE", options.id(), "TYPE", options.type(), "STATE", defaultState(options.state()), "NOW", now);
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "PARENT_FLOW_ID", options.parentFlowId());
        append(cmd, "ROOT_FLOW_ID", options.rootFlowId());
        append(cmd, "CORRELATION_ID", options.correlationId());
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendBool(cmd, "IDEMPOTENT", options.idempotent());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object enqueue(String id, CreateOptions options) {
        CreateOptions.Builder builder = CreateOptions.builder(id, options.type())
            .state(options.state() == null ? "queued" : options.state())
            .payload(options.payload())
            .partitionKey(options.partitionKey())
            .parentFlowId(options.parentFlowId())
            .rootFlowId(options.rootFlowId())
            .correlationId(options.correlationId())
            .runAtMs(options.runAtMs())
            .nowMs(options.nowMs())
            .values(options.values())
            .valueRefs(options.valueRefs())
            .returnRecord(options.returnRecord());
        if (options.priority() != null) {
            builder.priority(options.priority());
        }
        if (options.idempotent() != null) {
            builder.idempotent(options.idempotent());
        }
        if (options.retentionTtlMs() != null) {
            builder.retentionTtlMs(options.retentionTtlMs());
        }
        return create(builder.build());
    }

    public Object createMany(CreateManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        boolean anyPartition = options.items().stream().anyMatch(item -> item.partitionKey() != null && !item.partitionKey().isEmpty());
        String wirePartition = options.partitionKey() != null && !options.partitionKey().isEmpty()
            ? options.partitionKey()
            : anyPartition ? "MIXED" : "AUTO";
        List<Object> cmd = args("FLOW.CREATE_MANY", wirePartition, "TYPE", options.type(), "STATE", defaultState(options.state()), "NOW", now);
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendBool(cmd, "IDEMPOTENT", options.idempotent());
        appendBool(cmd, "INDEPENDENT", options.independent());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        boolean extended = options.items().stream().anyMatch(item -> !item.values().isEmpty() || !item.valueRefs().isEmpty());
        if (extended) {
            cmd.add("ITEMS_EXT");
            cmd.add(options.items().size());
            for (CreateItem item : options.items()) {
                cmd.add(item.id());
                cmd.add("MIXED".equals(wirePartition) ? requiredPartition(item) : "-");
                cmd.add(codec.encode(item.payload()));
                appendNamedCounts(cmd, item.values(), item.valueRefs());
            }
        } else {
            appendNamedValues(cmd, codec, options.values(), options.valueRefs());
            cmd.add("ITEMS");
            for (CreateItem item : options.items()) {
                cmd.add(item.id());
                if ("MIXED".equals(wirePartition)) {
                    cmd.add(requiredPartition(item));
                }
                cmd.add(codec.encode(item.payload()));
            }
        }
        return command(cmd);
    }

    public Object valuePut(Object value, String name, String ownerFlowId, String partitionKey, Long ttlMs) {
        List<Object> cmd = args("FLOW.VALUE.PUT", codec.encode(value), "NOW", nowMs());
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "OWNER_FLOW_ID", ownerFlowId);
        append(cmd, "NAME", name);
        append(cmd, "TTL", ttlMs);
        return command(cmd);
    }

    public List<Object> valueMGet(List<String> refs) {
        return valueMGet(refs, null);
    }

    public List<Object> valueMGet(List<String> refs, Long maxBytes) {
        if (refs.isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.VALUE.MGET");
        cmd.addAll(refs);
        append(cmd, "MAX_BYTES", maxBytes);
        return Resp.list(command(cmd)).stream().map(item -> item instanceof byte[] bytes ? codec.decode(bytes) : item).toList();
    }

    public Object signal(String id, String signal, String transitionTo, String partitionKey, Map<String, ?> values) {
        List<Object> cmd = args("FLOW.SIGNAL", id, "SIGNAL", signal);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "TRANSITION_TO", transitionTo);
        append(cmd, "NOW", nowMs());
        appendNamedValues(cmd, codec, values, Map.of());
        return command(cmd);
    }

    public List<FlowRecord> claimDue(ClaimDueOptions options) {
        List<Object> cmd = claimCommand("FLOW.CLAIM_DUE", options);
        return Resp.records(command(cmd), codec);
    }

    public List<FlowRecord> reclaim(ClaimDueOptions options) {
        List<Object> cmd = claimCommand("FLOW.RECLAIM", options);
        return Resp.records(command(cmd), codec);
    }

    public Object extendLease(String id, String leaseToken, long fencingToken, long leaseMs, String partitionKey) {
        List<Object> cmd = args("FLOW.EXTEND_LEASE", id, leaseToken, "FENCING", fencingToken, "LEASE_MS", leaseMs, "NOW", nowMs());
        append(cmd, "PARTITION", partitionKey);
        return command(cmd);
    }

    public Object transition(TransitionOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> cmd = args("FLOW.TRANSITION", options.id(), options.fromState(), options.toState(), "LEASE_TOKEN", options.leaseToken(), "FENCING", options.fencingToken(), "NOW", now);
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object complete(CompleteOptions options) {
        List<Object> cmd = args("FLOW.COMPLETE", options.id(), options.leaseToken(), "FENCING", options.fencingToken(), "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "RESULT", codec, options.result());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object retry(RetryOptions options) {
        List<Object> cmd = args("FLOW.RETRY", options.id(), options.leaseToken(), "FENCING", options.fencingToken(), "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object fail(FailOptions options) {
        List<Object> cmd = args("FLOW.FAIL", options.id(), options.leaseToken(), "FENCING", options.fencingToken(), "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object cancel(CancelOptions options) {
        List<Object> cmd = args("FLOW.CANCEL", options.id(), "FENCING", options.fencingToken(), "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "REASON", codec, options.reason());
        append(cmd, "TTL", options.ttlMs());
        appendReturnRecord(cmd, options.returnRecord());
        Object response = command(cmd);
        return options.returnRecord() ? Resp.optionalRecord(response, codec) : response;
    }

    public Object completeMany(CompleteManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.COMPLETE_MANY", options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "RESULT", codec, options.result());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object transitionMany(TransitionManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.TRANSITION_MANY", options.partitionKey() == null ? "MIXED" : options.partitionKey(), options.fromState(), options.toState());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        append(cmd, "PRIORITY", options.priority());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendFencedItems(cmd, options.partitionKey(), options.items(), true);
        return recordsOrResponse(command(cmd));
    }

    public Object retryMany(RetryManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.RETRY_MANY", options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object failMany(FailManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.FAIL_MANY", options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object cancelMany(CancelManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd = args("FLOW.CANCEL_MANY", options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "REASON", codec, options.reason());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendFencedItems(cmd, options.partitionKey(), options.items(), false);
        return recordsOrResponse(command(cmd));
    }

    public FlowRecord get(String id, String partitionKey) {
        List<Object> cmd = args("FLOW.GET", id);
        append(cmd, "PARTITION", partitionKey);
        return Resp.optionalRecord(command(cmd), codec);
    }

    public List<FlowRecord> list(String type, String state, String partitionKey, int count) {
        List<Object> cmd = args("FLOW.LIST", type);
        append(cmd, "STATE", state);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "COUNT", count == 0 ? null : count);
        return Resp.records(command(cmd), codec);
    }

    public Object rewind(String id, String toEvent, String partitionKey, String expectState, Long runAtMs, String reasonRef, Long nowMs, boolean returnRecord) {
        List<Object> cmd = args("FLOW.REWIND", id, "TO_EVENT", toEvent, "NOW", nowMs == null ? nowMs() : nowMs);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "EXPECT_STATE", expectState);
        append(cmd, "RUN_AT", runAtMs);
        append(cmd, "REASON_REF", reasonRef);
        appendReturnRecord(cmd, returnRecord);
        Object response = command(cmd);
        return returnRecord ? Resp.optionalRecord(response, codec) : response;
    }

    public List<FlowRecord> terminals(String type, String state, String partitionKey, int count) {
        List<Object> cmd = args("FLOW.TERMINALS", type);
        appendReadOptions(cmd, state, partitionKey, count, null, null, null);
        return Resp.records(command(cmd), codec);
    }

    public List<FlowRecord> failures(String type, String partitionKey, int count) {
        List<Object> cmd = args("FLOW.FAILURES", type);
        appendReadOptions(cmd, null, partitionKey, count, null, null, null);
        return Resp.records(command(cmd), codec);
    }

    public List<FlowRecord> byParent(String parentFlowId, String partitionKey, int count) {
        return indexQuery("FLOW.BY_PARENT", parentFlowId, partitionKey, count);
    }

    public List<FlowRecord> byRoot(String rootFlowId, String partitionKey, int count) {
        return indexQuery("FLOW.BY_ROOT", rootFlowId, partitionKey, count);
    }

    public List<FlowRecord> byCorrelation(String correlationId, String partitionKey, int count) {
        return indexQuery("FLOW.BY_CORRELATION", correlationId, partitionKey, count);
    }

    public List<FlowRecord> stuck(String type, String partitionKey, int count, Long olderThanMs, Long nowMs) {
        List<Object> cmd = args("FLOW.STUCK", type);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "COUNT", count == 0 ? null : count);
        append(cmd, "OLDER_THAN", olderThanMs);
        append(cmd, "NOW", nowMs);
        return Resp.records(command(cmd), codec);
    }

    public List<Object> history(String id, String partitionKey, int count) {
        List<Object> cmd = args("FLOW.HISTORY", id);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "COUNT", count == 0 ? null : count);
        return Resp.list(command(cmd));
    }

    public Map<String, Object> flowInfo(String type) {
        return Resp.parseKv(command("FLOW.INFO", type));
    }

    public Object spawnChildren(String parentId, List<ChildSpec> children, String partitionKey, String leaseToken, Long fencingToken) {
        SpawnChildrenOptions.Builder builder = SpawnChildrenOptions.builder(parentId, children)
            .partitionKey(partitionKey)
            .leaseToken(leaseToken);
        if (fencingToken != null) {
            builder.fencingToken(fencingToken);
        }
        return spawnChildren(builder.build());
    }

    public Object spawnChildren(SpawnChildrenOptions options) {
        List<Object> cmd = args("FLOW.SPAWN_CHILDREN", options.parentId(), "GROUP", options.groupId(), "WAIT", options.waitMode(), "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "FENCING", options.fencingToken());
        append(cmd, "WAIT_STATE", options.waitState());
        append(cmd, "SUCCESS", options.success());
        append(cmd, "FAILURE", options.failure());
        append(cmd, "FROM_STATE", options.fromState());
        append(cmd, "ON_CHILD_FAILED", options.onChildFailed());
        append(cmd, "ON_PARENT_CLOSED", options.onParentClosed());
        boolean mixed = options.children().stream().anyMatch(child -> child.partitionKey() != null && !child.partitionKey().isEmpty());
        boolean extended = options.children().stream().anyMatch(child -> !child.values().isEmpty() || !child.valueRefs().isEmpty());
        if (extended) {
            cmd.add("ITEMS_EXT");
            cmd.add(options.children().size());
            for (ChildSpec child : options.children()) {
                if (mixed && (child.partitionKey() == null || child.partitionKey().isEmpty())) {
                    throw new IllegalArgumentException("mixed spawnChildren items require partition key");
                }
                cmd.add(child.id());
                cmd.add(child.partitionKey() == null ? "-" : child.partitionKey());
                cmd.add(child.type());
                cmd.add(codec.encode(child.payload()));
                appendNamedCounts(cmd, mergeValues(options.values(), child.values()), mergeRefs(options.valueRefs(), child.valueRefs()));
            }
        } else {
            appendNamedValues(cmd, codec, options.values(), options.valueRefs());
            cmd.add("ITEMS");
            if (mixed) {
                cmd.add("MIXED");
            }
            for (ChildSpec child : options.children()) {
                cmd.add(child.id());
                if (mixed) {
                    if (child.partitionKey() == null || child.partitionKey().isEmpty()) {
                        throw new IllegalArgumentException("mixed spawnChildren items require partition key");
                    }
                    cmd.add(child.partitionKey());
                }
                cmd.add(child.type());
                cmd.add(codec.encode(child.payload()));
            }
        }
        return command(cmd);
    }

    public Object installPolicy(String type, String state, RetryPolicy retry, Long retentionTtlMs) {
        List<Object> cmd = args("FLOW.POLICY.SET", type);
        append(cmd, "STATE", state);
        if (retry != null) {
            append(cmd, "MAX_RETRIES", retry.maxRetries());
            append(cmd, "BACKOFF", retry.backoff());
            append(cmd, "BASE_MS", retry.baseMs());
            append(cmd, "MAX_MS", retry.maxMs());
            append(cmd, "JITTER_PCT", retry.jitterPct());
            append(cmd, "EXHAUSTED_TO", retry.exhaustedTo());
        }
        append(cmd, "RETENTION_TTL_MS", retentionTtlMs);
        return command(cmd);
    }

    public Map<String, Object> policyGet(String type, String state) {
        List<Object> cmd = args("FLOW.POLICY.GET", type);
        append(cmd, "STATE", state);
        return Resp.parseKv(command(cmd));
    }

    public Map<String, Object> retentionCleanup(Integer limit, Long nowMs) {
        List<Object> cmd = args("FLOW.RETENTION_CLEANUP");
        append(cmd, "LIMIT", limit);
        append(cmd, "NOW", nowMs);
        return Resp.parseKv(command(cmd));
    }

    public boolean cas(String key, Object expected, Object value, Long exSeconds) {
        List<Object> cmd = args("CAS", key, codec.encode(expected), codec.encode(value));
        append(cmd, "EX", exSeconds);
        Object response = command(cmd);
        return Boolean.TRUE.equals(response) || "1".equals(Resp.string(response));
    }

    public boolean lock(String key, String owner, long ttlMs) { return CommandArgs.ok(command("LOCK", key, owner, ttlMs)); }
    public long unlock(String key, String owner) { return Resp.number(command("UNLOCK", key, owner)); }
    public long extendLock(String key, String owner, long ttlMs) { return Resp.number(command("EXTEND", key, owner, ttlMs)); }
    public RateLimitResult ratelimitAdd(String key, long windowMs, long max, long count) {
        List<Object> response = Resp.list(command("RATELIMIT.ADD", key, windowMs, max, count));
        String status = response.isEmpty() ? "" : Resp.string(response.getFirst());
        long used = response.size() > 1 ? Resp.number(response.get(1)) : 0;
        long remaining = response.size() > 2 ? Resp.number(response.get(2)) : 0;
        long resetMs = response.size() > 3 ? Resp.number(response.get(3)) : 0;
        return new RateLimitResult(status, used, remaining, resetMs, "allowed".equals(status), Map.of("response", response));
    }
    public Map<String, Object> keyInfo(String key) { return Resp.parseKv(command("FERRICSTORE.KEY_INFO", key)); }
    public FetchOrComputeResult fetchOrCompute(String key, long ttlMs, String hint) {
        List<Object> response = Resp.list(hint == null ? command("FETCH_OR_COMPUTE", key, ttlMs) : command("FETCH_OR_COMPUTE", key, ttlMs, hint));
        String status = response.isEmpty() ? "" : Resp.string(response.getFirst());
        if ("hit".equals(status)) {
            Object value = response.size() > 1 && response.get(1) instanceof byte[] bytes ? codec.decode(bytes) : response.size() > 1 ? response.get(1) : null;
            return new FetchOrComputeResult(status, value, null, true, false);
        }
        String token = response.size() > 1 ? Resp.string(response.get(1)) : null;
        return new FetchOrComputeResult(status, null, token, false, true);
    }
    public boolean fetchOrComputeResult(String key, Object value, long ttlMs) { return CommandArgs.ok(command("FETCH_OR_COMPUTE_RESULT", key, codec.encode(value), ttlMs)); }
    public boolean fetchOrComputeError(String key, String message) { return CommandArgs.ok(command("FETCH_OR_COMPUTE_ERROR", key, message)); }
    public Map<String, Object> clusterHealth() { return Resp.parseKv(command("CLUSTER.HEALTH")); }
    public Map<String, Object> clusterStats() { return Resp.parseKv(command("CLUSTER.STATS")); }
    public long clusterKeyslot(String key) { return Resp.number(command("CLUSTER.KEYSLOT", key)); }
    public Object clusterSlots() { return command("CLUSTER.SLOTS"); }
    public Map<String, Object> clusterStatus() { return Resp.parseKv(command("CLUSTER.STATUS")); }
    public Object clusterRole() { return command("CLUSTER.ROLE"); }
    public boolean clusterJoin(String node, boolean replace) { return CommandArgs.ok(replace ? command("CLUSTER.JOIN", node, "REPLACE") : command("CLUSTER.JOIN", node)); }
    public boolean clusterLeave() { return CommandArgs.ok(command("CLUSTER.LEAVE")); }
    public boolean clusterFailover(long shardIndex, String targetNode) { return CommandArgs.ok(command("CLUSTER.FAILOVER", shardIndex, targetNode)); }
    public boolean clusterPromote(String node) { return CommandArgs.ok(command("CLUSTER.PROMOTE", node)); }
    public boolean clusterDemote(String node) { return CommandArgs.ok(command("CLUSTER.DEMOTE", node)); }
    public Object ferricstoreConfig(Object... args) { return command(prefix("FERRICSTORE.CONFIG", args)); }
    public Map<String, Object> ferricstoreMetrics(Object... args) { return Resp.parseKv(command(prefix("FERRICSTORE.METRICS", args))); }
    public Map<String, Object> ferricstoreHotness(Object... args) { return Resp.parseKv(command(prefix("FERRICSTORE.HOTNESS", args))); }
    public Object ferricstoreBlobgc(Object... args) { return command(prefix("FERRICSTORE.BLOBGC", args)); }
    public Object ferricstoreDoctor(Object... args) { return command(prefix("FERRICSTORE.DOCTOR", args)); }

    public String serverInfo(String section) {
        return Resp.string(section == null ? command("INFO") : command("INFO", section));
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

    private List<Object> claimCommand(String command, ClaimDueOptions options) {
        if (options.state() != null && !options.states().isEmpty()) {
            throw new IllegalArgumentException("state and states are mutually exclusive");
        }
        if (options.partitionKey() != null && !options.partitionKeys().isEmpty()) {
            throw new IllegalArgumentException("partitionKey and partitionKeys are mutually exclusive");
        }
        List<Object> cmd = args(command, options.type());
        if (options.states().isEmpty()) {
            append(cmd, "STATE", options.state());
        } else {
            options.states().forEach(state -> append(cmd, "STATE", state));
        }
        append(cmd, "WORKER", options.worker());
        append(cmd, "LEASE_MS", options.leaseMs() == 0 ? 30_000 : options.leaseMs());
        append(cmd, "LIMIT", options.limit() == 0 ? 1 : options.limit());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        if (!options.partitionKeys().isEmpty()) {
            cmd.add("PARTITIONS");
            cmd.add(options.partitionKeys().size());
            cmd.addAll(options.partitionKeys());
        }
        append(cmd, "PRIORITY", options.priority());
        append(cmd, "BLOCK", options.blockMs());
        appendPayloadRead(cmd, options.payload(), options.payloadMaxBytes());
        for (String value : options.values()) {
            append(cmd, "VALUE", value);
        }
        append(cmd, "VALUE_MAX_BYTES", options.valueMaxBytes());
        if (options.jobOnly()) {
            append(cmd, "RETURN", options.includeState() ? "JOBS_COMPACT_STATE" : "JOBS_COMPACT");
        }
        appendBool(cmd, "RECLAIM_EXPIRED", options.reclaimExpired());
        append(cmd, "RECLAIM_RATIO", options.reclaimRatio());
        return cmd;
    }

    private void appendNamedCounts(List<Object> cmd, Map<String, ?> values, Map<String, String> valueRefs) {
        cmd.add(values.size());
        values.forEach((name, value) -> {
            cmd.add(name);
            cmd.add(codec.encode(value));
        });
        cmd.add(valueRefs.size());
        valueRefs.forEach((name, ref) -> {
            cmd.add(name);
            cmd.add(ref);
        });
    }

    private void appendClaimedItems(List<Object> cmd, String partitionKey, List<ClaimedItem> items) {
        cmd.add("ITEMS");
        for (ClaimedItem item : items) {
            cmd.add(item.id());
            if (partitionKey == null) {
                cmd.add(item.partitionKey());
            }
            cmd.add(item.leaseToken());
            cmd.add(item.fencingToken());
        }
    }

    private void appendFencedItems(List<Object> cmd, String partitionKey, List<FencedItem> items, boolean includeLease) {
        cmd.add("ITEMS");
        for (FencedItem item : items) {
            cmd.add(item.id());
            if (partitionKey == null) {
                cmd.add(item.partitionKey());
            }
            if (includeLease) {
                cmd.add(item.leaseToken());
            }
            cmd.add(item.fencingToken());
        }
    }

    private Object recordsOrResponse(Object response) {
        if (response instanceof List<?> list && (list.isEmpty() || list.getFirst() instanceof Map<?, ?> || list.getFirst() instanceof List<?>)) {
            return Resp.records(response, codec);
        }
        return response;
    }

    private List<FlowRecord> indexQuery(String command, String key, String partitionKey, int count) {
        List<Object> cmd = args(command, key);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "COUNT", count == 0 ? null : count);
        return Resp.records(command(cmd), codec);
    }

    private static void appendReadOptions(List<Object> cmd, String state, String partitionKey, int count, Long fromMs, Long toMs, Boolean rev) {
        append(cmd, "STATE", state);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "COUNT", count == 0 ? null : count);
        append(cmd, "FROM_MS", fromMs);
        append(cmd, "TO_MS", toMs);
        appendBool(cmd, "REV", rev);
    }

    private static Map<String, Object> mergeValues(Map<String, ?> base, Map<String, ?> item) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (base != null) {
            base.forEach(merged::put);
        }
        if (item != null) {
            item.forEach(merged::put);
        }
        return merged;
    }

    private static Map<String, String> mergeRefs(Map<String, String> base, Map<String, String> item) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        if (item != null) {
            merged.putAll(item);
        }
        return merged;
    }

    private static void appendReturnRecord(List<Object> cmd, boolean returnRecord) {
        if (returnRecord) {
            cmd.add("RETURN");
            cmd.add("RECORD");
        }
    }

    private static List<Object> prefix(String command, Object[] rest) {
        List<Object> args = new ArrayList<>();
        args.add(command);
        args.addAll(List.of(rest));
        return args;
    }

    private static String requiredPartition(CreateItem item) {
        if (item.partitionKey() == null || item.partitionKey().isEmpty()) {
            throw new IllegalArgumentException("mixed createMany items require partition key");
        }
        return item.partitionKey();
    }

    private static long nowMs() {
        return System.currentTimeMillis();
    }

    private static String defaultState(String state) {
        return state == null || state.isEmpty() ? "queued" : state;
    }
}
