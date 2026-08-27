package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendBool;
import static com.ferricstore.CommandArgs.appendEncoded;
import static com.ferricstore.CommandArgs.appendEntries;
import static com.ferricstore.CommandArgs.appendMutationFields;
import static com.ferricstore.CommandArgs.appendNamedValues;
import static com.ferricstore.CommandArgs.appendNames;
import static com.ferricstore.CommandArgs.appendPayloadRead;
import static com.ferricstore.CommandArgs.args;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class FerricStoreClient implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final CommandExecutor executor;
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
    private final FlowSteps flowSteps;
    private final FlowInsights flowInsights;
    private final FlowSchedules flowSchedules;
    private final FlowGovernance flowGovernance;

    private FerricStoreClient(CommandExecutor executor, AutoCloseable closeable, Codec codec) {
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
        this.flowSteps = new FlowSteps(executor, this.codec);
        this.flowInsights = new FlowInsights(executor);
        this.flowSchedules = new FlowSchedules(executor);
        this.flowGovernance = new FlowGovernance(executor);
    }

    public static FerricStoreClient connect(String ferricUri) {
        return connect(ferricUri, new RawCodec());
    }

    public static FerricStoreClient connect(String ferricUri, Codec codec) {
        String scheme = endpointScheme(ferricUri);
        if ("http".equals(scheme) || "https".equals(scheme)) {
            return connect(ferricUri, codec, HttpTransportOptions.defaults());
        }
        if ("ferric".equals(scheme) || "ferrics".equals(scheme)) {
            return connect(ferricUri, codec, NativeTransportOptions.defaults());
        }
        throw unsupportedScheme();
    }

    /** Connects by URL scheme while applying HTTP-only transport options when applicable. */
    public static FerricStoreClient connect(
            String endpoint, Codec codec, HttpTransportOptions httpOptions) {
        String scheme = endpointScheme(endpoint);
        if ("http".equals(scheme) || "https".equals(scheme)) {
            HttpExecutor executor = HttpExecutor.connect(endpoint, httpOptions);
            return new FerricStoreClient(executor, executor, codec);
        }
        if ("ferric".equals(scheme) || "ferrics".equals(scheme)) {
            throw new IllegalArgumentException(
                    "HttpTransportOptions can only be used with http:// or https:// URLs");
        }
        throw unsupportedScheme();
    }

    /** Connects to native TCP/TLS with optional caller-provided TLS trust/key material. */
    public static FerricStoreClient connect(
            String endpoint, Codec codec, NativeTransportOptions nativeOptions) {
        if (nativeOptions == null) {
            throw new IllegalArgumentException("native transport options cannot be null");
        }
        String scheme = endpointScheme(endpoint);
        if ("ferric".equals(scheme) || "ferrics".equals(scheme)) {
            NativeExecutor executor = NativeExecutor.connectWithOptions(endpoint, nativeOptions);
            return new FerricStoreClient(executor, executor, codec);
        }
        if ("http".equals(scheme) || "https".equals(scheme)) {
            throw new IllegalArgumentException(
                    "NativeTransportOptions can only be used with ferric:// or ferrics:// URLs");
        }
        throw unsupportedScheme();
    }

    public static FerricStoreClient fromExecutor(CommandExecutor executor) {
        return fromExecutor(executor, new RawCodec());
    }

    public static FerricStoreClient fromExecutor(CommandExecutor executor, Codec codec) {
        return new FerricStoreClient(executor, null, codec);
    }

    public Codec codec() {
        return codec;
    }

    public Object command(Object... args) {
        return command(args(args));
    }

    public Object command(List<Object> args) {
        return executor.execute(copyArgs(args));
    }

    /** Executes any FerricStore command without blocking for its response. */
    public CompletableFuture<Object> commandAsync(Object... args) {
        return commandAsync(args(args));
    }

    /** Executes any FerricStore command without blocking for its response. */
    public CompletableFuture<Object> commandAsync(List<Object> args) {
        return executor.executeAsync(copyArgs(args));
    }

    /** Executes a catalogued Flow command with transport-independent arguments. */
    public Object command(FlowCommand command, Object... args) {
        List<Object> values = new ArrayList<>(args.length + 1);
        values.add(command.wireName());
        values.addAll(List.of(args));
        return executor.execute(values);
    }

    /** Executes a catalogued Flow command without blocking for its response. */
    public CompletableFuture<Object> commandAsync(FlowCommand command, Object... args) {
        List<Object> values = new ArrayList<>(args.length + 1);
        values.add(command.wireName());
        values.addAll(List.of(args));
        return executor.executeAsync(values);
    }

    public List<Object> pipeline(List<List<Object>> commands) {
        return executor.pipeline(commands.stream().map(FerricStoreClient::copyArgs).toList());
    }

    /** Executes a command pipeline without blocking for its response. */
    public CompletableFuture<List<Object>> pipelineAsync(List<List<Object>> commands) {
        return executor.pipelineAsync(commands.stream().map(FerricStoreClient::copyArgs).toList());
    }

    /** Executes an FQL1 query and returns its complete versioned result envelope. */
    public Map<String, Object> flowQuery(String query, Map<String, ?> params) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Flow query must not be blank");
        }
        return Resp.map(executor.flowQuery(query, Map.copyOf(params)));
    }

    /** Executes an FQL1 query without blocking for its response. */
    public CompletableFuture<Map<String, Object>> flowQueryAsync(
            String query, Map<String, ?> params) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Flow query must not be blank");
        }
        return AsyncFutures.map(executor.flowQueryAsync(query, Map.copyOf(params)), Resp::map);
    }

    public KeyValueStore kv() {
        return kv;
    }

    public HashStore hash() {
        return hash;
    }

    public ListStore lists() {
        return lists;
    }

    public SetStore sets() {
        return sets;
    }

    public SortedSetStore zset() {
        return zset;
    }

    public StreamStore stream() {
        return stream;
    }

    public BitmapStore bitmap() {
        return bitmap;
    }

    public HyperLogLogStore hyperloglog() {
        return hyperloglog;
    }

    public GeoStore geo() {
        return geo;
    }

    public JsonStore json() {
        return json;
    }

    public BloomFilterStore bloom() {
        return bloom;
    }

    public CuckooFilterStore cuckoo() {
        return cuckoo;
    }

    public CountMinSketchStore cms() {
        return cms;
    }

    public TopKStore topk() {
        return topk;
    }

    public TDigestStore tdigest() {
        return tdigest;
    }

    public FlowSteps flowSteps() {
        return flowSteps;
    }

    public FlowInsights flowInsights() {
        return flowInsights;
    }

    public FlowSchedules flowSchedules() {
        return flowSchedules;
    }

    public FlowGovernance flowGovernance() {
        return flowGovernance;
    }

    public FerricStoreTransaction transaction() {
        return transaction(List.of());
    }

    public FerricStoreTransaction transaction(List<String> watchKeys) {
        return new FerricStoreTransaction(sessionFactory(), codec, List.copyOf(watchKeys));
    }

    public FerricStorePubSub pubsubSession() {
        return new FerricStorePubSub(sessionFactory(), codec);
    }

    public Object ping() {
        return command("PING");
    }

    public Object ping(Object message) {
        return command("PING", message);
    }

    public Object echo(Object message) {
        return command("ECHO", message);
    }

    public long dbsize() {
        return Resp.number(command("DBSIZE"));
    }

    public boolean flushdb(Object... options) {
        return CommandArgs.ok(command(prefix("FLUSHDB", options)));
    }

    public boolean flushall(Object... options) {
        return CommandArgs.ok(command(prefix("FLUSHALL", options)));
    }

    public Object commandInfo(String... names) {
        List<Object> command = args("COMMAND");
        if (names.length > 0) {
            command.add("INFO");
            command.addAll(List.of(names));
        }
        return command(command);
    }

    public Object slowlog(String subcommand, Object... arguments) {
        List<Object> command = args("SLOWLOG", subcommand);
        command.addAll(List.of(arguments));
        return command(command);
    }

    public Object memory(String subcommand, Object... arguments) {
        List<Object> command = args("MEMORY", subcommand);
        command.addAll(List.of(arguments));
        return command(command);
    }

    public Object config(String subcommand, Object... arguments) {
        List<Object> command = args("CONFIG", subcommand);
        command.addAll(List.of(arguments));
        return command(command);
    }

    public long publish(String channel, Object message) {
        FlowValidation.requireText(channel, "channel");
        return Resp.number(command("PUBLISH", channel, codec.encode(message)));
    }

    public Object pubsub(String subcommand, Object... arguments) {
        List<Object> command = args("PUBSUB", subcommand);
        command.addAll(List.of(arguments));
        return command(command);
    }

    public Object create(CreateOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> cmd =
                args(
                        "FLOW.CREATE",
                        options.id(),
                        "TYPE",
                        options.type(),
                        "STATE",
                        defaultState(options.state()),
                        "NOW",
                        now);
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "PARENT_FLOW_ID", options.parentFlowId());
        append(cmd, "ROOT_FLOW_ID", options.rootFlowId());
        append(cmd, "CORRELATION_ID", options.correlationId());
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendBool(cmd, "IDEMPOTENT", options.idempotent());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        FlowMaxActive.append(cmd, options.maxActiveMs());
        appendEntries(cmd, "ATTRIBUTE", options.attributes());
        appendEntries(cmd, "STATE_META", options.stateMeta());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object enqueue(String id, CreateOptions options) {
        CreateOptions.Builder builder =
                CreateOptions.builder(id, options.type())
                        .state(options.state() == null ? "queued" : options.state())
                        .payload(options.payload())
                        .partitionKey(options.partitionKey())
                        .parentFlowId(options.parentFlowId())
                        .rootFlowId(options.rootFlowId())
                        .correlationId(options.correlationId())
                        .runAtMs(options.runAtMs())
                        .nowMs(options.nowMs())
                        .maxActiveMs(options.maxActiveMs())
                        .attributes(options.attributes())
                        .stateMeta(options.stateMeta())
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
        boolean anyPartition =
                options.items().stream()
                        .anyMatch(
                                item ->
                                        item.partitionKey() != null
                                                && !item.partitionKey().isEmpty());
        String wirePartition =
                options.partitionKey() != null && !options.partitionKey().isEmpty()
                        ? options.partitionKey()
                        : anyPartition ? "MIXED" : "AUTO";
        List<Object> cmd =
                args(
                        "FLOW.CREATE_MANY",
                        wirePartition,
                        "TYPE",
                        options.type(),
                        "STATE",
                        defaultState(options.state()),
                        "NOW",
                        now);
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendBool(cmd, "IDEMPOTENT", options.idempotent());
        appendBool(cmd, "INDEPENDENT", options.independent());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        FlowMaxActive.append(cmd, options.maxActiveMs());
        appendEntries(cmd, "ATTRIBUTE", options.attributes());
        appendEntries(cmd, "STATE_META", options.stateMeta());
        boolean mapped = options.items().stream().anyMatch(item -> item.maxActiveMs() != null);
        boolean extended =
                options.items().stream()
                        .anyMatch(item -> !item.values().isEmpty() || !item.valueRefs().isEmpty());
        if (mapped) {
            cmd.add("ITEMS_MAPS");
            cmd.add(options.items().size());
            for (CreateItem item : options.items()) {
                cmd.add(createItemMap(item, options));
            }
        } else if (extended) {
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

    public FlowRecord startAndClaim(StartAndClaimOptions options) {
        List<Object> cmd =
                args(
                        "FLOW.START_AND_CLAIM",
                        options.id(),
                        "TYPE",
                        options.type(),
                        "INITIAL_STATE",
                        options.initialState(),
                        "WORKER",
                        options.worker(),
                        "LEASE_MS",
                        options.leaseMs(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "PARENT_FLOW_ID", options.parentFlowId());
        append(cmd, "ROOT_FLOW_ID", options.rootFlowId());
        append(cmd, "CORRELATION_ID", options.correlationId());
        append(cmd, "PRIORITY", options.priority());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        FlowMaxActive.append(cmd, options.maxActiveMs());
        appendEntries(cmd, "ATTRIBUTE", options.attributes());
        appendEntries(cmd, "STATE_META", options.stateMeta());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        return Resp.optionalRecord(command(cmd), codec);
    }

    public Map<String, Object> valuePut(
            Object value, String name, String ownerFlowId, String partitionKey, Long ttlMs) {
        return valuePut(value, name, ownerFlowId, partitionKey, ttlMs, null);
    }

    public Map<String, Object> valuePut(
            Object value,
            String name,
            String ownerFlowId,
            String partitionKey,
            Long ttlMs,
            Boolean override) {
        List<Object> cmd = args("FLOW.VALUE.PUT", codec.encode(value), "NOW", nowMs());
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "OWNER_FLOW_ID", ownerFlowId);
        append(cmd, "NAME", name);
        appendBool(cmd, "OVERRIDE", override);
        append(cmd, "TTL", ttlMs);
        return Resp.parseKv(command(cmd));
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
        return Resp.list(command(cmd)).stream()
                .map(item -> item instanceof byte[] bytes ? codec.decode(bytes) : item)
                .toList();
    }

    public Object signal(
            String id,
            String signal,
            String transitionTo,
            String partitionKey,
            Map<String, ?> values) {
        return signal(id, signal, transitionTo, partitionKey, values, List.of());
    }

    public Object signal(
            String id,
            String signal,
            String transitionTo,
            String partitionKey,
            Map<String, ?> values,
            List<String> ifStates) {
        SignalOptions.Builder options = SignalOptions.builder(signal).partitionKey(partitionKey);
        ifStates.forEach(options::ifState);
        if (transitionTo != null) {
            options.transitionTo(transitionTo);
        }
        values.forEach(options::value);
        return signal(id, options.build());
    }

    public Object signal(String id, SignalOptions options) {
        List<Object> cmd = args("FLOW.SIGNAL", id, "SIGNAL", options.signal());
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "IDEMPOTENCY", options.idempotencyKey());
        options.ifStates().forEach(ifState -> append(cmd, "IF_STATE", ifState));
        append(cmd, "TRANSITION_TO", options.transitionTo());
        append(cmd, "RUN_AT", options.runAtMs());
        Object effectiveNow = options.nowMs();
        if (effectiveNow == null) {
            effectiveNow = nowMs();
        }
        append(cmd, "NOW", effectiveNow);
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendNames(cmd, "DROP_VALUE", options.dropValues());
        appendNames(cmd, "OVERRIDE_VALUE", options.overrideValues());
        return command(cmd);
    }

    public List<FlowRecord> claimDue(ClaimDueOptions options) {
        List<Object> cmd = claimCommand("FLOW.CLAIM_DUE", options);
        return Resp.records(command(cmd), codec);
    }

    public List<ClaimedItem> claimJobs(ClaimDueOptions options) {
        List<Object> cmd = claimCommand("FLOW.CLAIM_DUE", jobOnly(options));
        return Resp.claimedItems(command(cmd));
    }

    public List<FlowRecord> reclaim(ClaimDueOptions options) {
        List<Object> cmd = reclaimCommand(options);
        return Resp.records(command(cmd), codec);
    }

    public List<ClaimedItem> reclaimJobs(ClaimDueOptions options) {
        List<Object> cmd = reclaimCommand(jobOnly(options));
        return Resp.claimedItems(command(cmd));
    }

    public Object extendLease(
            String id, String leaseToken, long fencingToken, long leaseMs, String partitionKey) {
        List<Object> cmd =
                args(
                        "FLOW.EXTEND_LEASE",
                        id,
                        leaseToken,
                        "FENCING",
                        fencingToken,
                        "LEASE_MS",
                        leaseMs,
                        "NOW",
                        nowMs());
        append(cmd, "PARTITION", partitionKey);
        return command(cmd);
    }

    public Object transition(TransitionOptions options) {
        long now = options.nowMs() == 0 ? nowMs() : options.nowMs();
        long runAt = options.runAtMs() == 0 ? now : options.runAtMs();
        List<Object> cmd =
                args(
                        "FLOW.TRANSITION",
                        options.id(),
                        options.fromState(),
                        options.toState(),
                        "LEASE_TOKEN",
                        options.leaseToken(),
                        "FENCING",
                        options.fencingToken(),
                        "NOW",
                        now);
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", runAt);
        append(cmd, "PRIORITY", options.priority());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object complete(CompleteOptions options) {
        List<Object> cmd =
                args(
                        "FLOW.COMPLETE",
                        options.id(),
                        options.leaseToken(),
                        "FENCING",
                        options.fencingToken(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "RESULT", codec, options.result());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object retry(RetryOptions options) {
        requireRetryNamedValuesUnsupported(
                "FLOW.RETRY", options.values(), options.valueRefs(), options.mutationFields());
        List<Object> cmd =
                args(
                        "FLOW.RETRY",
                        options.id(),
                        options.leaseToken(),
                        "FENCING",
                        options.fencingToken(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object fail(FailOptions options) {
        List<Object> cmd =
                args(
                        "FLOW.FAIL",
                        options.id(),
                        options.leaseToken(),
                        "FENCING",
                        options.fencingToken(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object cancel(CancelOptions options) {
        List<Object> cmd =
                args(
                        "FLOW.CANCEL",
                        options.id(),
                        "FENCING",
                        options.fencingToken(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "PARTITION", options.partitionKey());
        appendEncoded(cmd, "REASON", codec, options.reason());
        append(cmd, "TTL", options.ttlMs());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        Object response = command(cmd);
        return options.returnRecord()
                ? recordOrGet(response, options.id(), options.partitionKey())
                : response;
    }

    public Object completeMany(CompleteManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd =
                args(
                        "FLOW.COMPLETE_MANY",
                        options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "RESULT", codec, options.result());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        if (options.returnOkOnSuccess()) {
            append(cmd, "RETURN", "OK_ON_SUCCESS");
        }
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object transitionMany(TransitionManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd =
                args(
                        "FLOW.TRANSITION_MANY",
                        options.partitionKey() == null ? "MIXED" : options.partitionKey(),
                        options.fromState(),
                        options.toState());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        append(cmd, "PRIORITY", options.priority());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        if (options.returnOkOnSuccess()) {
            append(cmd, "RETURN", "OK_ON_SUCCESS");
        }
        appendFencedItems(cmd, options.partitionKey(), options.items(), true);
        return recordsOrResponse(command(cmd));
    }

    public Object retryMany(RetryManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        requireRetryNamedValuesUnsupported(
                "FLOW.RETRY_MANY", options.values(), options.valueRefs(), options.mutationFields());
        List<Object> cmd =
                args(
                        "FLOW.RETRY_MANY",
                        options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "RUN_AT", options.runAtMs() == 0 ? null : options.runAtMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object failMany(FailManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd =
                args(
                        "FLOW.FAIL_MANY",
                        options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "ERROR", codec, options.error());
        appendEncoded(cmd, "PAYLOAD", codec, options.payload());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendMutationFields(cmd, options.mutationFields());
        appendNamedValues(cmd, codec, options.values(), options.valueRefs());
        appendClaimedItems(cmd, options.partitionKey(), options.items());
        return recordsOrResponse(command(cmd));
    }

    public Object cancelMany(CancelManyOptions options) {
        if (options.items().isEmpty()) {
            return List.of();
        }
        List<Object> cmd =
                args(
                        "FLOW.CANCEL_MANY",
                        options.partitionKey() == null ? "MIXED" : options.partitionKey());
        appendEncoded(cmd, "REASON", codec, options.reason());
        append(cmd, "TTL", options.ttlMs());
        append(cmd, "NOW", options.nowMs() == 0 ? nowMs() : options.nowMs());
        appendBool(cmd, "INDEPENDENT", options.independent());
        appendMutationFields(cmd, options.mutationFields());
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
        return queryRecords(FlowQueries.list(type, state, partitionKey, count));
    }

    public Object rewind(
            String id,
            String toEvent,
            String partitionKey,
            String expectState,
            Long runAtMs,
            String reasonRef,
            Long nowMs,
            boolean returnRecord) {
        long effectiveNowMs = nowMs == null ? nowMs() : nowMs;
        List<Object> cmd = args("FLOW.REWIND", id, "TO_EVENT", toEvent, "NOW", effectiveNowMs);
        append(cmd, "PARTITION", partitionKey);
        append(cmd, "EXPECT_STATE", expectState);
        append(cmd, "RUN_AT", runAtMs);
        append(cmd, "REASON_REF", reasonRef);
        Object response = command(cmd);
        if (!returnRecord) {
            return response;
        }
        FlowRecord record = get(id, partitionKey);
        if (record == null) {
            throw new FerricStoreException(
                    "FLOW command succeeded but record " + id + " was not found");
        }
        return record;
    }

    public List<FlowRecord> terminals(String type, String state, String partitionKey, int count) {
        return queryRecords(FlowQueries.terminals(type, state, partitionKey, count));
    }

    public List<FlowRecord> failures(String type, String partitionKey, int count) {
        return queryRecords(FlowQueries.failures(type, partitionKey, count));
    }

    public List<FlowRecord> byParent(String parentFlowId, String partitionKey, int count) {
        return queryRecords(
                FlowQueries.lineage("parent_flow_id", parentFlowId, partitionKey, count, "DESC"));
    }

    public List<FlowRecord> byRoot(String rootFlowId, String partitionKey, int count) {
        return queryRecords(
                FlowQueries.lineage("root_flow_id", rootFlowId, partitionKey, count, "ASC"));
    }

    public List<FlowRecord> byCorrelation(String correlationId, String partitionKey, int count) {
        return queryRecords(
                FlowQueries.lineage("correlation_id", correlationId, partitionKey, count, "DESC"));
    }

    public List<FlowRecord> stuck(
            String type, String partitionKey, int count, Long olderThanMs, Long nowMs) {
        long effectiveNow = nowMs == null ? nowMs() : nowMs;
        long effectiveAge = olderThanMs == null ? 0L : olderThanMs;
        return queryRecords(
                FlowQueries.stuck(type, partitionKey, count, effectiveAge, effectiveNow));
    }

    public List<Object> history(String id, String partitionKey, int count) {
        return history(
                id, HistoryOptions.builder().partitionKey(partitionKey).count(count).build());
    }

    public List<Object> history(String id, HistoryOptions options) {
        FlowValidation.requireText(id, "flow id");
        List<Object> cmd = args("FLOW.HISTORY", id, "COUNT", options.count());
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "FROM_EVENT", options.fromEvent());
        append(cmd, "TO_EVENT", options.toEvent());
        append(cmd, "FROM_MS", options.fromMs());
        append(cmd, "TO_MS", options.toMs());
        append(cmd, "FROM_VERSION", options.fromVersion());
        append(cmd, "TO_VERSION", options.toVersion());
        appendBool(cmd, "REV", options.reverse());
        append(cmd, "EVENT", options.event());
        append(cmd, "WORKER", options.worker());
        appendBool(cmd, "INCLUDE_COLD", options.includeCold());
        appendBool(cmd, "CONSISTENT_PROJECTION", options.consistentProjection());
        appendBool(cmd, "VALUES", options.values());
        append(cmd, "PAYLOAD_MAX_BYTES", options.payloadMaxBytes());
        return Resp.list(command(cmd));
    }

    public Map<String, Object> flowInfo(String type) {
        return Resp.parseKv(command("FLOW.INFO", type));
    }

    public Object spawnChildren(
            String parentFlowId,
            List<ChildSpec> children,
            String partitionKey,
            String leaseToken,
            Long fencingToken) {
        SpawnChildrenOptions.Builder builder =
                SpawnChildrenOptions.builder(parentFlowId, children)
                        .partitionKey(partitionKey)
                        .leaseToken(leaseToken);
        if (fencingToken != null) {
            builder.fencingToken(fencingToken);
        }
        return spawnChildren(builder.build());
    }

    public Object spawnChildren(SpawnChildrenOptions options) {
        List<Object> cmd =
                args(
                        "FLOW.SPAWN_CHILDREN",
                        options.parentFlowId(),
                        "GROUP",
                        options.groupId(),
                        "WAIT",
                        options.waitMode(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "FENCING", options.fencingToken());
        append(cmd, "WAIT_STATE", options.waitState());
        append(cmd, "SUCCESS", options.success());
        append(cmd, "FAILURE", options.failure());
        append(cmd, "FROM_STATE", options.fromState());
        append(cmd, "ON_CHILD_FAILED", options.onChildFailed());
        append(cmd, "ON_PARENT_CLOSED", options.onParentClosed());
        FlowMaxActive.append(cmd, options.maxActiveMs());
        boolean mixed =
                options.children().stream()
                        .anyMatch(
                                child ->
                                        child.partitionKey() != null
                                                && !child.partitionKey().isEmpty());
        boolean extended =
                options.children().stream()
                        .anyMatch(
                                child -> !child.values().isEmpty() || !child.valueRefs().isEmpty());
        if (mixed
                && options.children().stream()
                        .anyMatch(
                                child ->
                                        child.partitionKey() == null
                                                || child.partitionKey().isEmpty())) {
            throw new IllegalArgumentException("mixed spawnChildren items require partition key");
        }
        boolean mapped =
                mixed || options.children().stream().anyMatch(child -> child.maxActiveMs() != null);
        if (mapped) {
            cmd.add("ITEMS_MAPS");
            cmd.add(options.children().size());
            for (ChildSpec child : options.children()) {
                cmd.add(childItemMap(child, options));
            }
        } else if (extended) {
            cmd.add("ITEMS_EXT");
            cmd.add(options.children().size());
            for (ChildSpec child : options.children()) {
                cmd.add(child.id());
                cmd.add(child.partitionKey() == null ? "-" : child.partitionKey());
                cmd.add(child.type());
                cmd.add(codec.encode(child.payload()));
                appendNamedCounts(
                        cmd,
                        mergeValues(options.values(), child.values()),
                        mergeRefs(options.valueRefs(), child.valueRefs()));
            }
        } else {
            appendNamedValues(cmd, codec, options.values(), options.valueRefs());
            cmd.add("ITEMS");
            for (ChildSpec child : options.children()) {
                cmd.add(child.id());
                cmd.add(child.type());
                cmd.add(codec.encode(child.payload()));
            }
        }
        return command(cmd);
    }

    public Object installPolicy(String type, FlowPolicyOptions options) {
        List<Object> cmd = args("FLOW.POLICY.SET", type);
        append(cmd, "EXPECTED_GENERATION", options.expectedGeneration());
        appendBool(cmd, "REPLACE", options.replace());
        FlowMaxActive.append(cmd, options.maxActiveMs());
        if (options.indexedAttributesPresent()) {
            append(cmd, "INDEXED_ATTRIBUTES", json(options.indexedAttributes()));
        }
        append(cmd, "INDEXED_STATE_META", options.indexedStateMeta());
        append(cmd, "RETENTION_TTL_MS", options.retentionTtlMs());
        if (options.state() == null) {
            appendRetryPolicy(cmd, options.retry());
        } else {
            append(cmd, "STATE", options.state());
            append(cmd, "MODE", wireMode(options.mode()));
            appendRetryPolicy(cmd, options.retry());
        }
        for (Map.Entry<String, FlowStatePolicy> entry : options.states().entrySet()) {
            cmd.add("STATE");
            cmd.add(entry.getKey());
            FlowStatePolicy policy = entry.getValue();
            append(cmd, "MODE", wireMode(policy.mode()));
            appendRetryPolicy(cmd, policy.retry());
        }
        return command(cmd);
    }

    private static void appendRetryPolicy(List<Object> cmd, RetryPolicy retry) {
        if (retry == null) {
            return;
        }
        append(cmd, "MAX_RETRIES", retry.maxRetries());
        append(cmd, "BACKOFF", retry.backoff());
        append(cmd, "BASE_MS", retry.baseMs());
        append(cmd, "MAX_MS", retry.maxMs());
        append(cmd, "JITTER_PCT", retry.jitterPct());
        append(cmd, "EXHAUSTED_TO", retry.exhaustedTo());
    }

    private static String wireMode(FlowStateMode mode) {
        return mode == null ? null : mode.name();
    }

    public Map<String, Object> policyGet(String type, String state) {
        List<Object> cmd = args("FLOW.POLICY.GET", type);
        append(cmd, "STATE", state);
        return Resp.parseKv(command(cmd));
    }

    public Map<String, Object> effectReserve(
            String id, String effectKey, String effectType, EffectReserveOptions options) {
        FlowValidation.requireText(id, "flow id");
        FlowValidation.requireText(effectKey, "effect key");
        FlowValidation.requireText(effectType, "effect type");
        List<Object> cmd =
                args("FLOW.EFFECT.RESERVE", id, "EFFECT_KEY", effectKey, "EFFECT_TYPE", effectType);
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "FENCING", options.fencingToken());
        append(cmd, "OPERATION_DIGEST", options.operationDigest());
        append(cmd, "IDEMPOTENCY_KEY", options.idempotencyKey());
        append(cmd, "GOVERNANCE_SCOPE", options.governanceScope());
        append(cmd, "NOW", options.nowMs());
        return Resp.parseKv(command(cmd));
    }

    public Map<String, Object> effectConfirm(
            String id, String effectKey, EffectStatusOptions options) {
        return effectStatus("FLOW.EFFECT.CONFIRM", id, effectKey, options);
    }

    public Map<String, Object> effectFail(
            String id, String effectKey, EffectStatusOptions options) {
        return effectStatus("FLOW.EFFECT.FAIL", id, effectKey, options);
    }

    public Map<String, Object> effectCompensate(
            String id, String effectKey, EffectStatusOptions options) {
        return effectStatus("FLOW.EFFECT.COMPENSATE", id, effectKey, options);
    }

    public Map<String, Object> effectGet(String id, String effectKey, String partitionKey) {
        FlowValidation.requireText(id, "flow id");
        FlowValidation.requireText(effectKey, "effect key");
        List<Object> cmd = args("FLOW.EFFECT.GET", id, "EFFECT_KEY", effectKey);
        append(cmd, "PARTITION", partitionKey);
        Object response = command(cmd);
        if (response == null
                || response instanceof List<?> list && list.isEmpty()
                || response instanceof Map<?, ?> map && map.isEmpty()) {
            return null;
        }
        return Resp.parseKv(response);
    }

    private Map<String, Object> effectStatus(
            String command, String id, String effectKey, EffectStatusOptions options) {
        FlowValidation.requireText(id, "flow id");
        FlowValidation.requireText(effectKey, "effect key");
        List<Object> cmd = args(command, id, "EFFECT_KEY", effectKey);
        append(cmd, "PARTITION", options.partitionKey());
        append(cmd, "LEASE_TOKEN", options.leaseToken());
        append(cmd, "FENCING", options.fencingToken());
        append(cmd, "EXTERNAL_ID", options.externalId());
        append(cmd, "ERROR", options.error());
        append(cmd, "REASON", options.reason());
        append(cmd, "LATENCY_MS", options.latencyMs());
        append(cmd, "NOW", options.nowMs());
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

    public boolean lock(String key, String owner, long ttlMs) {
        return CommandArgs.ok(command("LOCK", key, owner, ttlMs));
    }

    public long unlock(String key, String owner) {
        return Resp.number(command("UNLOCK", key, owner));
    }

    public long extendLock(String key, String owner, long ttlMs) {
        return Resp.number(command("EXTEND", key, owner, ttlMs));
    }

    public RateLimitResult ratelimitAdd(String key, long windowMs, long max, long count) {
        List<Object> response = Resp.list(command("RATELIMIT.ADD", key, windowMs, max, count));
        String status = response.isEmpty() ? "" : Resp.string(response.get(0));
        long used = response.size() > 1 ? Resp.number(response.get(1)) : 0;
        long remaining = response.size() > 2 ? Resp.number(response.get(2)) : 0;
        long resetMs = response.size() > 3 ? Resp.number(response.get(3)) : 0;
        return new RateLimitResult(
                status,
                used,
                remaining,
                resetMs,
                "allowed".equals(status),
                Map.of("response", response));
    }

    public Map<String, Object> keyInfo(String key) {
        return Resp.parseKv(command("FERRICSTORE.KEY_INFO", key));
    }

    public FetchOrComputeResult fetchOrCompute(String key, long ttlMs, String hint) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("FETCH_OR_COMPUTE ttlMs must be positive");
        }
        List<Object> response =
                Resp.list(
                        hint == null
                                ? command("FETCH_OR_COMPUTE", key, ttlMs)
                                : command("FETCH_OR_COMPUTE", key, ttlMs, hint));
        String status = response.isEmpty() ? "" : Resp.string(response.get(0));
        if ("hit".equals(status)) {
            if (response.size() != 2) {
                throw new FerricStoreException(
                        "FETCH_OR_COMPUTE hit response must contain exactly two values");
            }
            Object value =
                    response.get(1) instanceof byte[] bytes ? codec.decode(bytes) : response.get(1);
            return new FetchOrComputeResult(status, value, null, null, true, false);
        }
        if (!"compute".equals(status) || response.size() != 3) {
            throw new FerricStoreException(
                    "FETCH_OR_COMPUTE compute response must be [compute, hint, ownership_token]");
        }
        String computeHint = Resp.string(response.get(1));
        Object token = ownershipToken(response.get(2));
        return new FetchOrComputeResult(status, null, computeHint, token, false, true);
    }

    public boolean fetchOrComputeResult(
            String key, Object ownershipToken, Object value, long ttlMs) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("FETCH_OR_COMPUTE_RESULT ttlMs must be positive");
        }
        return CommandArgs.ok(
                command(
                        "FETCH_OR_COMPUTE_RESULT",
                        key,
                        ownershipToken(ownershipToken),
                        codec.encode(value),
                        ttlMs));
    }

    public boolean fetchOrComputeError(String key, Object ownershipToken, String message) {
        return CommandArgs.ok(
                command("FETCH_OR_COMPUTE_ERROR", key, ownershipToken(ownershipToken), message));
    }

    public Map<String, Object> clusterHealth() {
        return Resp.parseKv(command("CLUSTER.HEALTH"));
    }

    public Map<String, Object> clusterStats() {
        return Resp.parseKv(command("CLUSTER.STATS"));
    }

    public long clusterKeyslot(String key) {
        return Resp.number(command("CLUSTER.KEYSLOT", key));
    }

    public Object clusterSlots() {
        return command("CLUSTER.SLOTS");
    }

    public Map<String, Object> clusterStatus() {
        return Resp.parseKv(command("CLUSTER.STATUS"));
    }

    public Object clusterRole() {
        return command("CLUSTER.ROLE");
    }

    public boolean clusterJoin(String node, boolean replace) {
        return CommandArgs.ok(
                replace ? command("CLUSTER.JOIN", node, "REPLACE") : command("CLUSTER.JOIN", node));
    }

    public boolean clusterLeave() {
        return CommandArgs.ok(command("CLUSTER.LEAVE"));
    }

    public boolean clusterFailover(long shardIndex, String targetNode) {
        return CommandArgs.ok(command("CLUSTER.FAILOVER", shardIndex, targetNode));
    }

    public boolean clusterPromote(String node) {
        return CommandArgs.ok(command("CLUSTER.PROMOTE", node));
    }

    public boolean clusterDemote(String node) {
        return CommandArgs.ok(command("CLUSTER.DEMOTE", node));
    }

    public Object ferricstoreConfig(Object... args) {
        return command(prefix("FERRICSTORE.CONFIG", args));
    }

    public Map<String, Object> ferricstoreMetrics(Object... args) {
        return Resp.parseKv(command(prefix("FERRICSTORE.METRICS", args)));
    }

    public Map<String, Object> ferricstoreHotness(Object... args) {
        return Resp.parseKv(command(prefix("FERRICSTORE.HOTNESS", args)));
    }

    public Object ferricstoreBlobgc(Object... args) {
        return command(prefix("FERRICSTORE.BLOBGC", args));
    }

    public Object ferricstoreDoctor(Object... args) {
        return command(prefix("FERRICSTORE.DOCTOR", args));
    }

    public String serverInfo(String section) {
        return Resp.string(section == null ? command("INFO") : command("INFO", section));
    }

    private SessionExecutorFactory sessionFactory() {
        if (executor instanceof SessionExecutorFactory factory) {
            return factory;
        }
        throw new IllegalStateException(
                "connection-affine sessions require the native TCP/TLS transport");
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
            throw new IllegalArgumentException(
                    "partitionKey and partitionKeys are mutually exclusive");
        }
        if (options.includeState() && !options.jobOnly()) {
            throw new IllegalArgumentException("includeState requires jobOnly=true");
        }
        if (options.includeAttributes() && !options.jobOnly()) {
            throw new IllegalArgumentException("includeAttributes requires jobOnly=true");
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
            append(cmd, "RETURN", compactReturnMode(options, true));
        }
        appendBool(cmd, "RECLAIM_EXPIRED", options.reclaimExpired());
        append(cmd, "RECLAIM_RATIO", options.reclaimRatio());
        return cmd;
    }

    private List<Object> reclaimCommand(ClaimDueOptions options) {
        if (!options.states().isEmpty()) {
            throw new IllegalArgumentException("FLOW.RECLAIM does not support states");
        }
        if (options.partitionKey() != null && !options.partitionKeys().isEmpty()) {
            throw new IllegalArgumentException(
                    "partitionKey and partitionKeys are mutually exclusive");
        }
        if (options.includeState() && !options.jobOnly()) {
            throw new IllegalArgumentException("includeState requires jobOnly=true");
        }
        if (options.includeAttributes() && !options.jobOnly()) {
            throw new IllegalArgumentException("includeAttributes requires jobOnly=true");
        }
        List<Object> cmd =
                args(
                        "FLOW.RECLAIM",
                        options.type(),
                        "WORKER",
                        options.worker(),
                        "LEASE_MS",
                        options.leaseMs() == 0 ? 30_000 : options.leaseMs(),
                        "LIMIT",
                        options.limit() == 0 ? 1 : options.limit(),
                        "NOW",
                        options.nowMs() == 0 ? nowMs() : options.nowMs());
        append(cmd, "PARTITION", options.partitionKey());
        if (!options.partitionKeys().isEmpty()) {
            cmd.add("PARTITIONS");
            cmd.add(options.partitionKeys().size());
            cmd.addAll(options.partitionKeys());
        }
        append(cmd, "PRIORITY", options.priority());
        appendPayloadRead(cmd, options.payload(), options.payloadMaxBytes());
        for (String value : options.values()) {
            append(cmd, "VALUE", value);
        }
        append(cmd, "VALUE_MAX_BYTES", options.valueMaxBytes());
        if (options.jobOnly()) {
            append(cmd, "RETURN", compactReturnMode(options, false));
        }
        return cmd;
    }

    private void appendNamedCounts(
            List<Object> cmd, Map<String, ?> values, Map<String, String> valueRefs) {
        cmd.add(values.size());
        values.forEach(
                (name, value) -> {
                    cmd.add(name);
                    cmd.add(codec.encode(value));
                });
        cmd.add(valueRefs.size());
        valueRefs.forEach(
                (name, ref) -> {
                    cmd.add(name);
                    cmd.add(ref);
                });
    }

    private Map<String, Object> createItemMap(CreateItem item, CreateManyOptions shared) {
        Map<String, Object> mapped = new LinkedHashMap<>();
        mapped.put("id", item.id());
        mapped.put("payload", codec.encode(item.payload()));
        if (item.partitionKey() != null && !item.partitionKey().isEmpty()) {
            mapped.put("partition_key", item.partitionKey());
        }
        putEncodedValues(mapped, mergeValues(shared.values(), item.values()));
        Map<String, String> refs = mergeRefs(shared.valueRefs(), item.valueRefs());
        if (!refs.isEmpty()) {
            mapped.put("value_refs", refs);
        }
        FlowMaxActive.put(mapped, item.maxActiveMs());
        return mapped;
    }

    private Map<String, Object> childItemMap(ChildSpec child, SpawnChildrenOptions shared) {
        Map<String, Object> mapped = new LinkedHashMap<>();
        mapped.put("id", child.id());
        mapped.put("type", child.type());
        mapped.put("payload", codec.encode(child.payload()));
        String partition =
                child.partitionKey() == null || child.partitionKey().isEmpty()
                        ? shared.partitionKey()
                        : child.partitionKey();
        if (partition != null && !partition.isEmpty()) {
            mapped.put("partition_key", partition);
        }
        putEncodedValues(mapped, mergeValues(shared.values(), child.values()));
        Map<String, String> refs = mergeRefs(shared.valueRefs(), child.valueRefs());
        if (!refs.isEmpty()) {
            mapped.put("value_refs", refs);
        }
        FlowMaxActive.put(mapped, child.maxActiveMs());
        return mapped;
    }

    private void putEncodedValues(Map<String, Object> mapped, Map<String, ?> values) {
        if (values.isEmpty()) {
            return;
        }
        Map<String, Object> encoded = new LinkedHashMap<>();
        values.forEach((name, value) -> encoded.put(name, codec.encode(value)));
        mapped.put("values", encoded);
    }

    private static void requireRetryNamedValuesUnsupported(
            String command,
            Map<String, ?> values,
            Map<String, String> valueRefs,
            FlowMutationFields mutationFields) {
        FlowMutationFields fields =
                mutationFields == null ? FlowMutationFields.empty() : mutationFields;
        if (!values.isEmpty()
                || !valueRefs.isEmpty()
                || !fields.dropValues().isEmpty()
                || !fields.overrideValues().isEmpty()) {
            throw new UnsupportedOperationException(
                    command + " does not support named-value mutations in FerricStore OSS");
        }
    }

    private void appendClaimedItems(
            List<Object> cmd, String partitionKey, List<ClaimedItem> items) {
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

    private void appendFencedItems(
            List<Object> cmd, String partitionKey, List<FencedItem> items, boolean includeLease) {
        cmd.add("ITEMS");
        for (FencedItem item : items) {
            cmd.add(item.id());
            if (partitionKey == null) {
                cmd.add(item.partitionKey());
            }
            cmd.add(item.fencingToken());
            if (includeLease) {
                cmd.add(item.leaseToken() == null ? "-" : item.leaseToken());
            }
        }
    }

    private Object recordsOrResponse(Object response) {
        if (response instanceof List<?> list
                && (list.isEmpty()
                        || list.get(0) instanceof Map<?, ?>
                        || list.get(0) instanceof List<?>)) {
            return Resp.records(response, codec);
        }
        return response;
    }

    @SuppressWarnings("PMD.AvoidCatchingNPE") // Preserve the established indexed validation error.
    private static List<Object> copyArgs(List<Object> args) {
        Objects.requireNonNull(args, "command args");
        try {
            return List.copyOf(args);
        } catch (NullPointerException error) {
            for (int index = 0; index < args.size(); index++) {
                if (args.get(index) == null) {
                    throw new IllegalArgumentException(
                            "Redis command argument cannot be null at index " + index, error);
                }
            }
            throw new IllegalArgumentException(
                    "Redis command arguments cannot contain null", error);
        }
    }

    private static String endpointScheme(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return "";
        }
        int separator = endpoint.indexOf(':');
        return separator <= 0 ? "" : endpoint.substring(0, separator).toLowerCase(Locale.ROOT);
    }

    private static IllegalArgumentException unsupportedScheme() {
        return new IllegalArgumentException(
                "FerricStore SDK URLs must use ferric://, ferrics://, http://, or https://");
    }

    private List<FlowRecord> queryRecords(FlowQueries.Request request) {
        Map<String, Object> response = flowQuery(request.query(), request.params());
        if (!"ferric.flow.query.result/v1".equals(Resp.string(response.get("version")))) {
            throw new FerricStoreException("expected ferric.flow.query.result/v1 response");
        }
        if (!response.containsKey("records")) {
            throw new FerricStoreException("Flow query response is missing records");
        }
        return Resp.records(response.get("records"), codec);
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

    private static Map<String, String> mergeRefs(
            Map<String, String> base, Map<String, String> item) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        if (item != null) {
            merged.putAll(item);
        }
        return merged;
    }

    private FlowRecord recordOrGet(Object response, String id, String partitionKey) {
        if (response instanceof Map<?, ?> || response instanceof List<?>) {
            FlowRecord returned = Resp.optionalRecord(response, codec);
            if (returned != null) {
                return returned;
            }
        }
        FlowRecord stored = get(id, partitionKey);
        if (stored == null) {
            throw new FerricStoreException(
                    "FLOW command succeeded but record " + id + " was not found");
        }
        return stored;
    }

    private static List<Object> prefix(String command, Object[] rest) {
        List<Object> args = new ArrayList<>();
        args.add(command);
        for (Object value : rest) {
            args.add(value);
        }
        return args;
    }

    private static ClaimDueOptions jobOnly(ClaimDueOptions options) {
        if (options.jobOnly()) {
            return options;
        }
        return new ClaimDueOptions(
                options.type(),
                options.state(),
                options.states(),
                options.worker(),
                options.partitionKey(),
                options.partitionKeys(),
                options.leaseMs(),
                options.limit(),
                options.nowMs(),
                options.blockMs(),
                options.priority(),
                options.reclaimExpired(),
                options.reclaimRatio(),
                options.payload(),
                options.payloadMaxBytes(),
                options.values(),
                options.valueMaxBytes(),
                true,
                options.includeState(),
                options.includeAttributes());
    }

    private static String compactReturnMode(ClaimDueOptions options, boolean includeStateAllowed) {
        if (includeStateAllowed && options.includeState() && options.includeAttributes()) {
            return "JOBS_COMPACT_STATE_ATTRS";
        }
        if (includeStateAllowed && options.includeState()) {
            return "JOBS_COMPACT_STATE";
        }
        return options.includeAttributes() ? "JOBS_COMPACT_ATTRS" : "JOBS_COMPACT";
    }

    private static String json(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("Flow policy value is not JSON-compatible", error);
        }
    }

    private static String requiredPartition(CreateItem item) {
        if (item.partitionKey() == null || item.partitionKey().isEmpty()) {
            throw new IllegalArgumentException("mixed createMany items require partition key");
        }
        return item.partitionKey();
    }

    private static Object ownershipToken(Object value) {
        if (value instanceof String token && !token.isEmpty()) {
            return token;
        }
        if (value instanceof byte[] token && token.length > 0) {
            return token;
        }
        throw new IllegalArgumentException(
                "fetch_or_compute ownership token must be a non-empty string or byte array");
    }

    private static long nowMs() {
        return System.currentTimeMillis();
    }

    private static String defaultState(String state) {
        return state == null || state.isEmpty() ? "queued" : state;
    }
}
