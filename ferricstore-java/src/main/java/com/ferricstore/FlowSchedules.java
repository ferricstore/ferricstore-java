package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendBool;
import static com.ferricstore.CommandArgs.args;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Durable Flow schedule management. */
public final class FlowSchedules {
    private static final Set<String> KINDS = Set.of("one_shot", "delay", "interval", "cron");
    private static final Set<String> LIST_STATES =
            Set.of("active", "paused", "running", "completed", "failed", "cancelled", "all");
    private static final Set<String> OVERLAP_POLICIES =
            Set.of("allow", "skip", "queue_after_previous", "fail_schedule");
    private static final Set<String> TARGET_FIELDS =
            Set.of(
                    "type",
                    "state",
                    "id",
                    "id_prefix",
                    "partition_key",
                    "correlation_id",
                    "parent_flow_id",
                    "root_flow_id",
                    "run_at_ms",
                    "priority",
                    "payload",
                    "payload_ref",
                    "values",
                    "value_refs");

    private final CommandExecutor executor;

    FlowSchedules(CommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public Map<String, Object> create(String id, CreateOptions options) {
        FlowValidation.requireText(id, "schedule id");
        Objects.requireNonNull(options, "options");
        List<Object> command = args(FlowCommand.SCHEDULE_CREATE.wireName(), id);
        append(command, "KIND", options.kind());
        append(command, "AT_MS", options.atMs());
        append(command, "DELAY_MS", options.delayMs());
        append(command, "START_AT_MS", options.startAtMs());
        append(command, "EVERY_MS", options.everyMs());
        append(command, "CRON", options.cron());
        append(command, "TIMEZONE", options.timezone());
        append(command, "TARGET", options.target());
        append(command, "CATCHUP_POLICY", options.catchupPolicy());
        append(command, "OVERLAP_POLICY", options.overlapPolicy());
        append(command, "OVERLAP_RETRY_MS", options.overlapRetryMs());
        append(command, "MAX_FIRES", options.maxFires());
        append(command, "END_AT_MS", options.endAtMs());
        appendBool(command, "OVERWRITE", options.overwrite());
        append(command, "NOW", options.nowMs());
        options.extraOptions()
                .forEach((name, value) -> append(command, name.toUpperCase(Locale.ROOT), value));
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> get(String id) {
        FlowValidation.requireText(id, "schedule id");
        return Resp.optionalMap(executor.execute(args(FlowCommand.SCHEDULE_GET.wireName(), id)));
    }

    public Map<String, Object> fire(String id, Long fireAtMs, Long nowMs) {
        List<Object> command = scheduleOperation(FlowCommand.SCHEDULE_FIRE, id, nowMs);
        FlowValidation.requireOptionalExactNonNegative(fireAtMs, "fireAtMs");
        append(command, "FIRE_AT_MS", fireAtMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> pause(String id, Long nowMs) {
        return Resp.parseKv(
                executor.execute(scheduleOperation(FlowCommand.SCHEDULE_PAUSE, id, nowMs)));
    }

    public Map<String, Object> resume(String id, Long nowMs) {
        return Resp.parseKv(
                executor.execute(scheduleOperation(FlowCommand.SCHEDULE_RESUME, id, nowMs)));
    }

    public void delete(String id, Long nowMs) {
        Object response =
                executor.execute(scheduleOperation(FlowCommand.SCHEDULE_DELETE, id, nowMs));
        if (!CommandArgs.ok(response)) {
            throw new FerricStoreException("FLOW.SCHEDULE.DELETE response must be OK");
        }
    }

    public Map<String, Object> fireDue(FireDueOptions options) {
        FireDueOptions effective = options == null ? FireDueOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.SCHEDULE_FIRE_DUE.wireName());
        append(command, "NOW", effective.nowMs());
        append(command, "WORKER", effective.worker());
        append(command, "LEASE_MS", effective.leaseMs());
        append(command, "BLOCK", effective.blockMs());
        append(command, "LIMIT", effective.limit());
        return Resp.parseKv(executor.execute(command));
    }

    public List<Map<String, Object>> list(ListOptions options) {
        ListOptions effective = options == null ? ListOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.SCHEDULE_LIST.wireName());
        append(command, "KIND", effective.kind());
        append(command, "STATE", effective.state());
        append(command, "TIMEZONE", effective.timezone());
        append(command, "TARGET_TYPE", effective.targetType());
        append(command, "FROM_MS", effective.fromMs());
        append(command, "TO_MS", effective.toMs());
        append(command, "COUNT", effective.count());
        appendBool(command, "REV", effective.reverse());
        return Resp.maps(executor.execute(command));
    }

    private static List<Object> scheduleOperation(FlowCommand command, String id, Long nowMs) {
        FlowValidation.requireText(id, "schedule id");
        FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
        List<Object> args = args(command.wireName(), id);
        append(args, "NOW", nowMs);
        return args;
    }

    public record CreateOptions(
            Map<String, ?> target,
            String kind,
            Long atMs,
            Long delayMs,
            Long startAtMs,
            Long everyMs,
            String cron,
            String timezone,
            String catchupPolicy,
            String overlapPolicy,
            Long overlapRetryMs,
            Long maxFires,
            Long endAtMs,
            Boolean overwrite,
            Long nowMs,
            Map<String, ?> extraOptions) {
        public CreateOptions {
            target = ImmutableCopies.map(target);
            extraOptions = ImmutableCopies.map(extraOptions);
            validateTarget(target);
            kind =
                    validateTiming(
                            kind,
                            atMs,
                            delayMs,
                            startAtMs,
                            everyMs,
                            cron,
                            timezone,
                            catchupPolicy,
                            overlapPolicy,
                            overlapRetryMs,
                            maxFires,
                            endAtMs,
                            nowMs,
                            target);
        }

        public static CreateOptionsBuilder builder(Map<String, ?> target) {
            return new CreateOptionsBuilder(target);
        }
    }

    public static final class CreateOptionsBuilder {
        private final Map<String, ?> target;
        private String kind;
        private Long atMs;
        private Long delayMs;
        private Long startAtMs;
        private Long everyMs;
        private String cron;
        private String timezone;
        private String catchupPolicy;
        private String overlapPolicy;
        private Long overlapRetryMs;
        private Long maxFires;
        private Long endAtMs;
        private Boolean overwrite;
        private Long nowMs;
        private final Map<String, Object> extraOptions = new LinkedHashMap<>();

        private CreateOptionsBuilder(Map<String, ?> target) {
            this.target = target;
        }

        public CreateOptionsBuilder kind(String value) {
            kind = value;
            return this;
        }

        public CreateOptionsBuilder atMs(long value) {
            atMs = value;
            return this;
        }

        public CreateOptionsBuilder delayMs(long value) {
            delayMs = value;
            return this;
        }

        public CreateOptionsBuilder startAtMs(long value) {
            startAtMs = value;
            return this;
        }

        public CreateOptionsBuilder everyMs(long value) {
            everyMs = value;
            return this;
        }

        public CreateOptionsBuilder cron(String value) {
            cron = value;
            return this;
        }

        public CreateOptionsBuilder timezone(String value) {
            timezone = value;
            return this;
        }

        public CreateOptionsBuilder catchupPolicy(String value) {
            catchupPolicy = value;
            return this;
        }

        public CreateOptionsBuilder overlapPolicy(String value) {
            overlapPolicy = value;
            return this;
        }

        public CreateOptionsBuilder overlapRetryMs(long value) {
            overlapRetryMs = value;
            return this;
        }

        public CreateOptionsBuilder maxFires(long value) {
            maxFires = value;
            return this;
        }

        public CreateOptionsBuilder endAtMs(long value) {
            endAtMs = value;
            return this;
        }

        public CreateOptionsBuilder overwrite(boolean value) {
            overwrite = value;
            return this;
        }

        public CreateOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public CreateOptionsBuilder extra(String name, Object value) {
            FlowValidation.requireText(name, "schedule option name");
            extraOptions.put(name, value);
            return this;
        }

        public CreateOptions build() {
            return new CreateOptions(
                    target,
                    kind,
                    atMs,
                    delayMs,
                    startAtMs,
                    everyMs,
                    cron,
                    timezone,
                    catchupPolicy,
                    overlapPolicy,
                    overlapRetryMs,
                    maxFires,
                    endAtMs,
                    overwrite,
                    nowMs,
                    extraOptions);
        }
    }

    public record FireDueOptions(
            Long nowMs, String worker, Long leaseMs, Long blockMs, Integer limit) {
        public FireDueOptions {
            FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
            if (worker != null) {
                FlowValidation.requireText(worker, "worker");
            }
            FlowValidation.requireOptionalExactPositive(leaseMs, "leaseMs");
            FlowValidation.requireOptionalNonNegative(blockMs, "blockMs");
            if (limit != null) {
                FlowValidation.requirePositive(limit, "limit");
            }
            long effectiveLeaseMs = leaseMs == null ? 30_000 : leaseMs;
            if (nowMs != null && nowMs > FlowValidation.MAX_EXACT_INTEGER - effectiveLeaseMs) {
                throw new IllegalArgumentException(
                        "nowMs plus leaseMs exceeds exact integer range");
            }
        }

        public static FireDueOptionsBuilder builder() {
            return new FireDueOptionsBuilder();
        }
    }

    public static final class FireDueOptionsBuilder {
        private Long nowMs;
        private String worker;
        private Long leaseMs;
        private Long blockMs;
        private Integer limit;

        private FireDueOptionsBuilder() {}

        public FireDueOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public FireDueOptionsBuilder worker(String value) {
            worker = value;
            return this;
        }

        public FireDueOptionsBuilder leaseMs(long value) {
            leaseMs = value;
            return this;
        }

        public FireDueOptionsBuilder blockMs(long value) {
            blockMs = value;
            return this;
        }

        public FireDueOptionsBuilder limit(int value) {
            limit = value;
            return this;
        }

        public FireDueOptions build() {
            return new FireDueOptions(nowMs, worker, leaseMs, blockMs, limit);
        }
    }

    public record ListOptions(
            String kind,
            String state,
            String timezone,
            String targetType,
            Long fromMs,
            Long toMs,
            Integer count,
            Boolean reverse) {
        public ListOptions {
            if (kind != null && !KINDS.contains(kind)) {
                throw new IllegalArgumentException(
                        "kind must be one_shot, delay, interval, or cron");
            }
            if (state != null && !LIST_STATES.contains(state)) {
                throw new IllegalArgumentException("invalid schedule state");
            }
            FlowValidation.requireOptionalExactNonNegative(fromMs, "fromMs");
            FlowValidation.requireOptionalExactNonNegative(toMs, "toMs");
            if (fromMs != null && toMs != null && fromMs > toMs) {
                throw new IllegalArgumentException("fromMs must not exceed toMs");
            }
            if (count != null) {
                FlowValidation.requirePositive(count, "count");
            }
        }

        public static ListOptionsBuilder builder() {
            return new ListOptionsBuilder();
        }
    }

    public static final class ListOptionsBuilder {
        private String kind;
        private String state;
        private String timezone;
        private String targetType;
        private Long fromMs;
        private Long toMs;
        private Integer count;
        private Boolean reverse;

        private ListOptionsBuilder() {}

        public ListOptionsBuilder kind(String value) {
            kind = value;
            return this;
        }

        public ListOptionsBuilder state(String value) {
            state = value;
            return this;
        }

        public ListOptionsBuilder timezone(String value) {
            timezone = value;
            return this;
        }

        public ListOptionsBuilder targetType(String value) {
            targetType = value;
            return this;
        }

        public ListOptionsBuilder fromMs(long value) {
            fromMs = value;
            return this;
        }

        public ListOptionsBuilder toMs(long value) {
            toMs = value;
            return this;
        }

        public ListOptionsBuilder count(int value) {
            count = value;
            return this;
        }

        public ListOptionsBuilder reverse(boolean value) {
            reverse = value;
            return this;
        }

        public ListOptions build() {
            return new ListOptions(kind, state, timezone, targetType, fromMs, toMs, count, reverse);
        }
    }

    private static void validateTarget(Map<String, ?> target) {
        if (target.isEmpty()) {
            throw new IllegalArgumentException("target must contain a non-empty type");
        }
        if (!TARGET_FIELDS.containsAll(target.keySet())) {
            throw new IllegalArgumentException("target contains an unknown field");
        }
        Object type = target.get("type");
        if (!(type instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("target type must not be blank");
        }
        if ("__ferricstore_schedule".equals(type)) {
            throw new IllegalArgumentException("target type is reserved for internal use");
        }
        if (target.get("id") != null && target.get("id_prefix") != null) {
            throw new IllegalArgumentException("target cannot set both id and id_prefix");
        }
    }

    private static String validateTiming(
            String requestedKind,
            Long atMs,
            Long delayMs,
            Long startAtMs,
            Long everyMs,
            String cron,
            String timezone,
            String catchupPolicy,
            String overlapPolicy,
            Long overlapRetryMs,
            Long maxFires,
            Long endAtMs,
            Long nowMs,
            Map<String, ?> target) {
        String kind =
                requestedKind != null
                        ? requestedKind
                        : cron != null
                                ? "cron"
                                : everyMs != null
                                        ? "interval"
                                        : delayMs != null ? "delay" : "one_shot";
        if (!KINDS.contains(kind)) {
            throw new IllegalArgumentException("kind must be one_shot, delay, interval, or cron");
        }
        if (atMs != null && startAtMs != null) {
            throw new IllegalArgumentException("cannot set both atMs and startAtMs");
        }
        FlowValidation.requireOptionalExactNonNegative(atMs, "atMs");
        FlowValidation.requireOptionalExactNonNegative(delayMs, "delayMs");
        FlowValidation.requireOptionalExactNonNegative(startAtMs, "startAtMs");
        FlowValidation.requireOptionalExactPositive(everyMs, "everyMs");
        FlowValidation.requireOptionalExactPositive(overlapRetryMs, "overlapRetryMs");
        FlowValidation.requireOptionalExactPositive(maxFires, "maxFires");
        FlowValidation.requireOptionalExactNonNegative(endAtMs, "endAtMs");
        FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
        if ("delay".equals(kind) && delayMs == null) {
            throw new IllegalArgumentException("delayMs is required for delay schedules");
        }
        if ("interval".equals(kind) && everyMs == null) {
            throw new IllegalArgumentException("everyMs is required for interval schedules");
        }
        if ("cron".equals(kind)) {
            FlowValidation.requireText(cron, "cron");
        } else if (timezone != null || cron != null) {
            throw new IllegalArgumentException(
                    "cron and timezone are only supported for cron schedules");
        }
        boolean recurring = "interval".equals(kind) || "cron".equals(kind);
        if (recurring && target.get("id") != null) {
            throw new IllegalArgumentException("recurring schedule target must use id_prefix");
        }
        if (!recurring && (overlapPolicy != null || maxFires != null || endAtMs != null)) {
            throw new IllegalArgumentException("recurring options require interval or cron kind");
        }
        if (overlapPolicy != null && !OVERLAP_POLICIES.contains(overlapPolicy)) {
            throw new IllegalArgumentException("invalid overlap policy");
        }
        if (overlapRetryMs != null && !"queue_after_previous".equals(overlapPolicy)) {
            throw new IllegalArgumentException("overlapRetryMs requires queue_after_previous");
        }
        if (catchupPolicy != null
                && !("interval".equals(kind) && "fire_once".equals(catchupPolicy))) {
            throw new IllegalArgumentException("catchupPolicy fire_once requires interval kind");
        }
        return kind;
    }
}
