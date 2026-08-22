package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendEncoded;
import static com.ferricstore.CommandArgs.args;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Atomic Flow step-continuation and deterministic multi-step operations. */
public final class FlowSteps {
    private final CommandExecutor executor;
    private final Codec codec;

    FlowSteps(CommandExecutor executor, Codec codec) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    /** Transitions a leased run and acquires the next step in one durable command. */
    public Object continueStep(ContinueOptions options) {
        Objects.requireNonNull(options, "options");
        Object effectiveNow = options.nowMs();
        if (effectiveNow == null) {
            effectiveNow = System.currentTimeMillis();
        }
        List<Object> command =
                args(
                        FlowCommand.STEP_CONTINUE.wireName(),
                        options.id(),
                        options.leaseToken(),
                        options.fromState(),
                        options.toState(),
                        "FENCING",
                        options.fencingToken(),
                        "LEASE_MS",
                        options.leaseMs(),
                        "NOW",
                        effectiveNow);
        append(command, "PARTITION", options.partitionKey());
        append(command, "WORKER", options.worker());
        appendEncoded(command, "PAYLOAD", codec, options.payload());
        if (options.returnJob()) {
            command.add("RETURN");
            command.add("JOBS_COMPACT");
        }
        appendEntries(command, "ATTRIBUTE_MERGE", options.attributesMerge());
        appendNames(command, "ATTRIBUTE_DELETE", options.attributesDelete());
        appendEntries(command, "STATE_META", options.stateMeta());
        CommandArgs.appendNamedValues(command, codec, options.values(), options.valueRefs());
        appendNames(command, "DROP_VALUE", options.dropValues());
        appendNames(command, "OVERRIDE_VALUE", options.overrideValues());

        Object response = executor.execute(command);
        if (options.returnJob()) {
            return Resp.claimedItem(response);
        }
        if (response instanceof Map<?, ?> || response instanceof List<?>) {
            return Resp.record(response, codec);
        }
        List<Object> get = args(FlowCommand.GET.wireName(), options.id());
        append(get, "PARTITION", options.partitionKey());
        FlowRecord record = Resp.optionalRecord(executor.execute(get), codec);
        if (record == null) {
            throw new FerricStoreException(
                    "FLOW.STEP_CONTINUE succeeded but record " + options.id() + " was not found");
        }
        return record;
    }

    /** Runs a deterministic state chain for multiple new run IDs in one durable command. */
    public Object runMany(RunManyOptions options) {
        Objects.requireNonNull(options, "options");
        List<Object> command = args(FlowCommand.RUN_STEPS_MANY.wireName(), "TYPE", options.type());
        if (!options.states().isEmpty()) {
            command.add("STATES");
            command.add(options.states());
        } else {
            command.add("STEPS");
            command.add(options.steps());
        }
        command.add("WORKER");
        command.add(options.worker());
        command.add("LEASE_MS");
        command.add(options.leaseMs());
        command.add("NOW");
        Object effectiveNow = options.nowMs();
        command.add(effectiveNow == null ? System.currentTimeMillis() : effectiveNow);
        appendEncoded(command, "PAYLOAD", codec, options.payload());
        appendEncoded(command, "RESULT", codec, options.result());
        append(command, "RETENTION_TTL_MS", options.retentionTtlMs());
        command.add("ITEMS");
        command.add(
                options.items().stream()
                        .map(
                                item -> {
                                    Map<String, Object> value = new LinkedHashMap<>();
                                    value.put("id", item.id());
                                    if (item.partitionKey() != null) {
                                        value.put("partition_key", item.partitionKey());
                                    }
                                    return Map.copyOf(value);
                                })
                        .toList());
        return executor.execute(command);
    }

    private static void appendEntries(List<Object> command, String option, Map<String, ?> values) {
        values.forEach(
                (name, value) -> {
                    command.add(option);
                    command.add(name);
                    command.add(value);
                });
    }

    private static void appendNames(List<Object> command, String option, List<String> names) {
        names.forEach(
                name -> {
                    command.add(option);
                    command.add(name);
                });
    }

    public record RunItem(String id, String partitionKey) {
        public RunItem {
            FlowValidation.requireText(id, "run_steps_many item id");
            if (partitionKey != null) {
                FlowValidation.requireText(partitionKey, "run_steps_many item partition key");
            }
        }
    }

    public record ContinueOptions(
            String id,
            String leaseToken,
            long fencingToken,
            String fromState,
            String toState,
            long leaseMs,
            String partitionKey,
            Object payload,
            Map<String, ?> values,
            Map<String, String> valueRefs,
            List<String> dropValues,
            List<String> overrideValues,
            Map<String, ?> attributesMerge,
            List<String> attributesDelete,
            Map<String, ?> stateMeta,
            Long nowMs,
            String worker,
            boolean returnJob) {
        public ContinueOptions {
            FlowValidation.requireText(id, "flow id");
            FlowValidation.requireText(leaseToken, "flow lease token");
            FlowValidation.requireFencingToken(fencingToken);
            FlowValidation.requireText(fromState, "from state");
            FlowValidation.requireText(toState, "to state");
            FlowValidation.requirePositive(leaseMs, "leaseMs");
            FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
            values = ImmutableCopies.map(values);
            valueRefs = ImmutableCopies.map(valueRefs);
            dropValues = ImmutableCopies.list(dropValues);
            overrideValues = ImmutableCopies.list(overrideValues);
            attributesMerge = ImmutableCopies.map(attributesMerge);
            attributesDelete = ImmutableCopies.list(attributesDelete);
            stateMeta = ImmutableCopies.map(stateMeta);
        }

        public static ContinueBuilder builder(
                String id, String leaseToken, long fencingToken, String fromState, String toState) {
            return new ContinueBuilder(id, leaseToken, fencingToken, fromState, toState);
        }
    }

    public static final class ContinueBuilder {
        private final String id;
        private final String leaseToken;
        private final long fencingToken;
        private final String fromState;
        private final String toState;
        private long leaseMs = 30_000;
        private String partitionKey;
        private Object payload;
        private final Map<String, Object> values = new LinkedHashMap<>();
        private final Map<String, String> valueRefs = new LinkedHashMap<>();
        private final List<String> dropValues = new ArrayList<>();
        private final List<String> overrideValues = new ArrayList<>();
        private final Map<String, Object> attributesMerge = new LinkedHashMap<>();
        private final List<String> attributesDelete = new ArrayList<>();
        private final Map<String, Object> stateMeta = new LinkedHashMap<>();
        private Long nowMs;
        private String worker;
        private boolean returnJob;

        private ContinueBuilder(
                String id, String leaseToken, long fencingToken, String fromState, String toState) {
            this.id = id;
            this.leaseToken = leaseToken;
            this.fencingToken = fencingToken;
            this.fromState = fromState;
            this.toState = toState;
        }

        public ContinueBuilder leaseMs(long value) {
            leaseMs = value;
            return this;
        }

        public ContinueBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public ContinueBuilder payload(Object value) {
            payload = value;
            return this;
        }

        public ContinueBuilder value(String name, Object value) {
            values.put(name, value);
            return this;
        }

        public ContinueBuilder valueRef(String name, String value) {
            valueRefs.put(name, value);
            return this;
        }

        public ContinueBuilder dropValue(String name) {
            dropValues.add(name);
            return this;
        }

        public ContinueBuilder overrideValue(String name) {
            overrideValues.add(name);
            return this;
        }

        public ContinueBuilder attribute(String name, Object value) {
            attributesMerge.put(name, value);
            return this;
        }

        public ContinueBuilder deleteAttribute(String name) {
            attributesDelete.add(name);
            return this;
        }

        public ContinueBuilder stateMeta(String name, Object value) {
            stateMeta.put(name, value);
            return this;
        }

        public ContinueBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public ContinueBuilder worker(String value) {
            worker = value;
            return this;
        }

        public ContinueBuilder returnJob(boolean value) {
            returnJob = value;
            return this;
        }

        public ContinueOptions build() {
            return new ContinueOptions(
                    id,
                    leaseToken,
                    fencingToken,
                    fromState,
                    toState,
                    leaseMs,
                    partitionKey,
                    payload,
                    values,
                    valueRefs,
                    dropValues,
                    overrideValues,
                    attributesMerge,
                    attributesDelete,
                    stateMeta,
                    nowMs,
                    worker,
                    returnJob);
        }
    }

    public record RunManyOptions(
            String type,
            List<RunItem> items,
            List<String> states,
            Integer steps,
            String worker,
            long leaseMs,
            Long nowMs,
            Object payload,
            Object result,
            Long retentionTtlMs) {
        public RunManyOptions {
            FlowValidation.requireText(type, "flow type");
            items = ImmutableCopies.list(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("run_steps_many items must not be empty");
            }
            states = ImmutableCopies.list(states);
            if (states.isEmpty() == (steps == null)) {
                throw new IllegalArgumentException(
                        "run_steps_many requires exactly one of states or steps");
            }
            FlowValidation.requireTextList(states, "run_steps_many states", true);
            if (steps != null) {
                FlowValidation.requirePositive(steps, "run_steps_many steps");
            }
            FlowValidation.requireText(worker, "worker");
            FlowValidation.requirePositive(leaseMs, "leaseMs");
            FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
            FlowValidation.requireOptionalPositive(retentionTtlMs, "retentionTtlMs");
        }

        public static RunManyBuilder builder(String type, List<RunItem> items) {
            return new RunManyBuilder(type, items);
        }
    }

    public static final class RunManyBuilder {
        private final String type;
        private final List<RunItem> items;
        private List<String> states = List.of();
        private Integer steps;
        private String worker;
        private long leaseMs = 30_000;
        private Long nowMs;
        private Object payload;
        private Object result;
        private Long retentionTtlMs;

        private RunManyBuilder(String type, List<RunItem> items) {
            this.type = type;
            this.items = List.copyOf(items);
        }

        public RunManyBuilder states(List<String> value) {
            states = List.copyOf(value);
            steps = null;
            return this;
        }

        public RunManyBuilder steps(int value) {
            steps = value;
            states = List.of();
            return this;
        }

        public RunManyBuilder worker(String value) {
            worker = value;
            return this;
        }

        public RunManyBuilder leaseMs(long value) {
            leaseMs = value;
            return this;
        }

        public RunManyBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public RunManyBuilder payload(Object value) {
            payload = value;
            return this;
        }

        public RunManyBuilder result(Object value) {
            result = value;
            return this;
        }

        public RunManyBuilder retentionTtlMs(long value) {
            retentionTtlMs = value;
            return this;
        }

        public RunManyOptions build() {
            return new RunManyOptions(
                    type,
                    items,
                    states,
                    steps,
                    worker,
                    leaseMs,
                    nowMs,
                    payload,
                    result,
                    retentionTtlMs);
        }
    }
}
