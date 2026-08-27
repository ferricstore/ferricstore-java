package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendBool;
import static com.ferricstore.CommandArgs.args;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Flow governance ledger, approvals, circuits, budgets, and distributed limits. */
public final class FlowGovernance {
    private static final int MAX_LIMIT_MUTATION_AMOUNT = 1_000;
    private static final Set<String> APPROVAL_STATUSES = Set.of("pending", "approved", "rejected");

    private final CommandExecutor executor;

    FlowGovernance(CommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public List<Map<String, Object>> ledger(String id, LedgerOptions options) {
        FlowValidation.requireText(id, "flow id");
        LedgerOptions effective = options == null ? LedgerOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.GOVERNANCE_LEDGER.wireName(), id);
        append(command, "PARTITION", effective.partitionKey());
        append(command, "LIMIT", effective.limit());
        append(command, "FROM_MS", effective.fromMs());
        append(command, "TO_MS", effective.toMs());
        appendBool(command, "REV", effective.reverse());
        return Resp.maps(executor.execute(command));
    }

    public Map<String, Object> approvalRequest(String id, ApprovalRequestOptions options) {
        FlowValidation.requireText(id, "approval id");
        Objects.requireNonNull(options, "options");
        List<Object> command = args(FlowCommand.APPROVAL_REQUEST.wireName(), id);
        append(command, "FLOW_ID", options.flowId());
        append(command, "SCOPE", options.scope());
        append(command, "REASON", options.reason());
        append(command, "REQUESTED_BY", options.requestedBy());
        if (!options.assignees().isEmpty()) {
            append(command, "ASSIGNEES", options.assignees());
        }
        append(command, "POLICY_HASH", options.policyHash());
        append(command, "POLICY_VERSION", options.policyVersion());
        append(command, "TIMEOUT_MS", options.timeoutMs());
        append(command, "EXPIRES_AT_MS", options.expiresAtMs());
        append(command, "NOW", options.nowMs());
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> approvalApprove(
            String id, String approver, String reason, Long nowMs) {
        return approvalDecision(FlowCommand.APPROVAL_APPROVE, id, approver, reason, nowMs);
    }

    public Map<String, Object> approvalReject(
            String id, String approver, String reason, Long nowMs) {
        return approvalDecision(FlowCommand.APPROVAL_REJECT, id, approver, reason, nowMs);
    }

    public Map<String, Object> approvalGet(String id) {
        FlowValidation.requireText(id, "approval id");
        return Resp.optionalMap(executor.execute(args(FlowCommand.APPROVAL_GET.wireName(), id)));
    }

    public List<Map<String, Object>> approvalList(ApprovalListOptions options) {
        ApprovalListOptions effective =
                options == null ? ApprovalListOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.APPROVAL_LIST.wireName());
        appendApprovalFilters(command, effective);
        return Resp.maps(executor.execute(command));
    }

    public Map<String, Object> overview(ApprovalListOptions options) {
        ApprovalListOptions effective =
                options == null ? ApprovalListOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.GOVERNANCE_OVERVIEW.wireName());
        appendApprovalFilters(command, effective);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> circuitOpen(String scope, CircuitOpenOptions options) {
        FlowValidation.requireText(scope, "scope");
        CircuitOpenOptions effective =
                options == null ? CircuitOpenOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.CIRCUIT_OPEN.wireName(), scope);
        append(command, "OPEN_MS", effective.openMs());
        append(command, "FAILURE_THRESHOLD", effective.failureThreshold());
        append(command, "WINDOW_MS", effective.windowMs());
        append(command, "MIN_CALLS", effective.minCalls());
        append(command, "FAILURE_RATE_PCT", effective.failureRatePct());
        append(command, "LATENCY_THRESHOLD_MS", effective.latencyThresholdMs());
        if (!effective.errorClasses().isEmpty()) {
            append(command, "ERROR_CLASSES", effective.errorClasses());
        }
        append(command, "HALF_OPEN_MAX_PROBES", effective.halfOpenMaxProbes());
        append(command, "HALF_OPEN_SUCCESS_THRESHOLD", effective.halfOpenSuccessThreshold());
        append(command, "NOW", effective.nowMs());
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> circuitClose(String scope, Long nowMs) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
        List<Object> command = args(FlowCommand.CIRCUIT_CLOSE.wireName(), scope);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> circuitGet(String scope) {
        FlowValidation.requireText(scope, "scope");
        return Resp.optionalMap(executor.execute(args(FlowCommand.CIRCUIT_GET.wireName(), scope)));
    }

    public Map<String, Object> budgetReserve(
            String scope, long amount, BudgetReserveOptions options) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requirePositive(amount, "amount");
        BudgetReserveOptions effective =
                options == null ? BudgetReserveOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.BUDGET_RESERVE.wireName(), scope);
        append(command, "AMOUNT", amount);
        append(command, "LIMIT", effective.limit());
        append(command, "WINDOW_MS", effective.windowMs());
        append(command, "RESERVATION_ID", effective.reservationId());
        append(command, "NOW", effective.nowMs());
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> budgetCommit(
            String scope,
            String reservationId,
            long actualAmount,
            Map<String, ?> usage,
            Long nowMs) {
        validateBudgetSettlement(scope, reservationId, nowMs);
        FlowValidation.requireNonNegative(actualAmount, "actualAmount");
        List<Object> command = args(FlowCommand.BUDGET_COMMIT.wireName(), scope);
        append(command, "RESERVATION_ID", reservationId);
        append(command, "ACTUAL_AMOUNT", actualAmount);
        append(command, "USAGE", usage);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> budgetRelease(String scope, String reservationId, Long nowMs) {
        validateBudgetSettlement(scope, reservationId, nowMs);
        List<Object> command = args(FlowCommand.BUDGET_RELEASE.wireName(), scope);
        append(command, "RESERVATION_ID", reservationId);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> budgetGet(String scope) {
        FlowValidation.requireText(scope, "scope");
        return Resp.optionalMap(executor.execute(args(FlowCommand.BUDGET_GET.wireName(), scope)));
    }

    public List<Map<String, Object>> budgetList(FilterOptions options) {
        FilterOptions effective = options == null ? FilterOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.BUDGET_LIST.wireName());
        appendFilter(command, effective, false);
        return Resp.maps(executor.execute(command));
    }

    public Map<String, Object> limitLease(
            String scope, long shardId, long amount, long ttlMs, Long limit, Long nowMs) {
        validateLimitMutation(scope, shardId, amount, nowMs);
        FlowValidation.requirePositive(ttlMs, "ttlMs");
        if (ttlMs > FlowValidation.MAX_EXACT_INTEGER) {
            throw new IllegalArgumentException("ttlMs exceeds exact integer range");
        }
        FlowValidation.requireOptionalNonNegative(limit, "limit");
        if (nowMs != null && nowMs > FlowValidation.MAX_EXACT_INTEGER - ttlMs) {
            throw new IllegalArgumentException("nowMs plus ttlMs exceeds exact integer range");
        }
        List<Object> command = args(FlowCommand.LIMIT_LEASE.wireName(), scope);
        append(command, "SHARD_ID", shardId);
        append(command, "AMOUNT", amount);
        append(command, "LIMIT", limit);
        append(command, "TTL_MS", ttlMs);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> limitSpend(String scope, long shardId, long amount, Long nowMs) {
        validateLimitMutation(scope, shardId, amount, nowMs);
        List<Object> command = args(FlowCommand.LIMIT_SPEND.wireName(), scope);
        append(command, "SHARD_ID", shardId);
        append(command, "AMOUNT", amount);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> limitRelease(
            String scope, long shardId, List<String> reservationIds, Long nowMs) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requireNonNegative(shardId, "shardId");
        FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
        FlowValidation.requireUniqueTextList(
                reservationIds, "reservationIds", MAX_LIMIT_MUTATION_AMOUNT, 256);
        List<Object> command = args(FlowCommand.LIMIT_RELEASE.wireName(), scope);
        append(command, "SHARD_ID", shardId);
        append(command, "RESERVATION_IDS", List.copyOf(reservationIds));
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    public Map<String, Object> limitGet(String scope, Long nowMs) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
        List<Object> command = args(FlowCommand.LIMIT_GET.wireName(), scope);
        append(command, "NOW", nowMs);
        return Resp.optionalMap(executor.execute(command));
    }

    public List<Map<String, Object>> limitList(FilterOptions options) {
        FilterOptions effective = options == null ? FilterOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.LIMIT_LIST.wireName());
        appendFilter(command, effective, true);
        return Resp.maps(executor.execute(command));
    }

    private Map<String, Object> approvalDecision(
            FlowCommand flowCommand, String id, String approver, String reason, Long nowMs) {
        FlowValidation.requireText(id, "approval id");
        FlowValidation.requireText(approver, "approver");
        FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
        List<Object> command = args(flowCommand.wireName(), id);
        append(command, "APPROVER", approver);
        append(command, "REASON", reason);
        append(command, "NOW", nowMs);
        return Resp.parseKv(executor.execute(command));
    }

    private static void appendApprovalFilters(List<Object> command, ApprovalListOptions options) {
        append(command, "STATUS", options.status());
        append(command, "SCOPE", options.scope());
        append(command, "PARTITION", options.partitionKey());
        append(command, "FLOW_ID", options.flowId());
        append(command, "LIMIT", options.limit());
    }

    private static void appendFilter(
            List<Object> command, FilterOptions options, boolean includeNow) {
        append(command, "SCOPE", options.scope());
        append(command, "PARTITION", options.partitionKey());
        append(command, "LIMIT", options.limit());
        if (includeNow) {
            append(command, "NOW", options.nowMs());
        }
    }

    private static void validateBudgetSettlement(String scope, String reservationId, Long nowMs) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requireText(reservationId, "reservationId");
        FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
    }

    private static void validateLimitMutation(String scope, long shardId, long amount, Long nowMs) {
        FlowValidation.requireText(scope, "scope");
        FlowValidation.requireNonNegative(shardId, "shardId");
        FlowValidation.requirePositive(amount, "amount");
        if (amount > MAX_LIMIT_MUTATION_AMOUNT) {
            throw new IllegalArgumentException("amount cannot exceed " + MAX_LIMIT_MUTATION_AMOUNT);
        }
        FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
    }

    public record LedgerOptions(
            String partitionKey, Integer limit, Long fromMs, Long toMs, Boolean reverse) {
        public LedgerOptions {
            if (limit != null) {
                FlowValidation.requirePositive(limit, "limit");
            }
            FlowValidation.requireOptionalNonNegative(fromMs, "fromMs");
            FlowValidation.requireOptionalNonNegative(toMs, "toMs");
            if (fromMs != null && toMs != null && fromMs > toMs) {
                throw new IllegalArgumentException("fromMs must not exceed toMs");
            }
        }

        public static LedgerOptionsBuilder builder() {
            return new LedgerOptionsBuilder();
        }
    }

    public static final class LedgerOptionsBuilder {
        private String partitionKey;
        private Integer limit;
        private Long fromMs;
        private Long toMs;
        private Boolean reverse;

        private LedgerOptionsBuilder() {}

        public LedgerOptionsBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public LedgerOptionsBuilder limit(int value) {
            limit = value;
            return this;
        }

        public LedgerOptionsBuilder fromMs(long value) {
            fromMs = value;
            return this;
        }

        public LedgerOptionsBuilder toMs(long value) {
            toMs = value;
            return this;
        }

        public LedgerOptionsBuilder reverse(boolean value) {
            reverse = value;
            return this;
        }

        public LedgerOptions build() {
            return new LedgerOptions(partitionKey, limit, fromMs, toMs, reverse);
        }
    }

    public record ApprovalRequestOptions(
            String flowId,
            String scope,
            String reason,
            String requestedBy,
            List<String> assignees,
            String policyHash,
            Object policyVersion,
            Long timeoutMs,
            Long expiresAtMs,
            Long nowMs) {
        public ApprovalRequestOptions {
            FlowValidation.requireText(flowId, "flowId");
            FlowValidation.requireText(scope, "scope");
            assignees = ImmutableCopies.list(assignees);
            FlowValidation.requireTextList(assignees, "assignees", true);
            FlowValidation.requireOptionalPositive(timeoutMs, "timeoutMs");
            FlowValidation.requireOptionalNonNegative(expiresAtMs, "expiresAtMs");
            FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
            if (policyVersion != null
                    && !(policyVersion instanceof String || policyVersion instanceof Number)) {
                throw new IllegalArgumentException("policyVersion must be text or a number");
            }
        }

        public static ApprovalRequestOptionsBuilder builder(String flowId, String scope) {
            return new ApprovalRequestOptionsBuilder(flowId, scope);
        }
    }

    public static final class ApprovalRequestOptionsBuilder {
        private final String flowId;
        private final String scope;
        private String reason;
        private String requestedBy;
        private List<String> assignees = List.of();
        private String policyHash;
        private Object policyVersion;
        private Long timeoutMs;
        private Long expiresAtMs;
        private Long nowMs;

        private ApprovalRequestOptionsBuilder(String flowId, String scope) {
            this.flowId = flowId;
            this.scope = scope;
        }

        public ApprovalRequestOptionsBuilder reason(String value) {
            reason = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder requestedBy(String value) {
            requestedBy = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder assignees(List<String> value) {
            assignees = List.copyOf(value);
            return this;
        }

        public ApprovalRequestOptionsBuilder policyHash(String value) {
            policyHash = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder policyVersion(Object value) {
            policyVersion = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder timeoutMs(long value) {
            timeoutMs = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder expiresAtMs(long value) {
            expiresAtMs = value;
            return this;
        }

        public ApprovalRequestOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public ApprovalRequestOptions build() {
            return new ApprovalRequestOptions(
                    flowId,
                    scope,
                    reason,
                    requestedBy,
                    assignees,
                    policyHash,
                    policyVersion,
                    timeoutMs,
                    expiresAtMs,
                    nowMs);
        }
    }

    public record ApprovalListOptions(
            String status, String scope, String partitionKey, String flowId, Integer limit) {
        public ApprovalListOptions {
            if (status != null && !APPROVAL_STATUSES.contains(status)) {
                throw new IllegalArgumentException("status must be pending, approved, or rejected");
            }
            if (scope != null) {
                FlowValidation.requireText(scope, "scope");
            } else if (partitionKey != null) {
                FlowValidation.requireText(partitionKey, "partitionKey");
            }
            if (flowId != null) {
                FlowValidation.requireText(flowId, "flowId");
            }
            if (limit != null) {
                FlowValidation.requirePositive(limit, "limit");
            }
        }

        public static ApprovalListOptionsBuilder builder() {
            return new ApprovalListOptionsBuilder();
        }
    }

    public static final class ApprovalListOptionsBuilder {
        private String status;
        private String scope;
        private String partitionKey;
        private String flowId;
        private Integer limit;

        private ApprovalListOptionsBuilder() {}

        public ApprovalListOptionsBuilder status(String value) {
            status = value;
            return this;
        }

        public ApprovalListOptionsBuilder scope(String value) {
            scope = value;
            return this;
        }

        public ApprovalListOptionsBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public ApprovalListOptionsBuilder flowId(String value) {
            flowId = value;
            return this;
        }

        public ApprovalListOptionsBuilder limit(int value) {
            limit = value;
            return this;
        }

        public ApprovalListOptions build() {
            return new ApprovalListOptions(status, scope, partitionKey, flowId, limit);
        }
    }

    public record CircuitOpenOptions(
            Long openMs,
            Integer failureThreshold,
            Long windowMs,
            Integer minCalls,
            Integer failureRatePct,
            Long latencyThresholdMs,
            List<String> errorClasses,
            Integer halfOpenMaxProbes,
            Integer halfOpenSuccessThreshold,
            Long nowMs) {
        public CircuitOpenOptions {
            FlowValidation.requireOptionalPositive(openMs, "openMs");
            if (failureThreshold != null) {
                FlowValidation.requirePositive(failureThreshold, "failureThreshold");
            }
            FlowValidation.requireOptionalPositive(windowMs, "windowMs");
            if (minCalls != null) {
                FlowValidation.requirePositive(minCalls, "minCalls");
                if (minCalls > 64) {
                    throw new IllegalArgumentException("minCalls cannot exceed 64");
                }
            }
            if (failureRatePct != null && (failureRatePct < 1 || failureRatePct > 100)) {
                throw new IllegalArgumentException("failureRatePct must be between 1 and 100");
            }
            FlowValidation.requireOptionalPositive(latencyThresholdMs, "latencyThresholdMs");
            if (halfOpenMaxProbes != null) {
                FlowValidation.requirePositive(halfOpenMaxProbes, "halfOpenMaxProbes");
            }
            if (halfOpenSuccessThreshold != null) {
                FlowValidation.requirePositive(
                        halfOpenSuccessThreshold, "halfOpenSuccessThreshold");
            }
            FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
            errorClasses = ImmutableCopies.list(errorClasses);
            FlowValidation.requireTextList(errorClasses, "errorClasses", true);
            int threshold = failureThreshold == null ? 5 : failureThreshold;
            if (threshold > 64 && (failureRatePct == null || minCalls == null)) {
                throw new IllegalArgumentException(
                        "failureThreshold above 64 requires failureRatePct and minCalls");
            }
        }

        public static CircuitOpenOptionsBuilder builder() {
            return new CircuitOpenOptionsBuilder();
        }
    }

    public static final class CircuitOpenOptionsBuilder {
        private Long openMs;
        private Integer failureThreshold;
        private Long windowMs;
        private Integer minCalls;
        private Integer failureRatePct;
        private Long latencyThresholdMs;
        private List<String> errorClasses = List.of();
        private Integer halfOpenMaxProbes;
        private Integer halfOpenSuccessThreshold;
        private Long nowMs;

        private CircuitOpenOptionsBuilder() {}

        public CircuitOpenOptionsBuilder openMs(long value) {
            openMs = value;
            return this;
        }

        public CircuitOpenOptionsBuilder failureThreshold(int value) {
            failureThreshold = value;
            return this;
        }

        public CircuitOpenOptionsBuilder windowMs(long value) {
            windowMs = value;
            return this;
        }

        public CircuitOpenOptionsBuilder minCalls(int value) {
            minCalls = value;
            return this;
        }

        public CircuitOpenOptionsBuilder failureRatePct(int value) {
            failureRatePct = value;
            return this;
        }

        public CircuitOpenOptionsBuilder latencyThresholdMs(long value) {
            latencyThresholdMs = value;
            return this;
        }

        public CircuitOpenOptionsBuilder errorClasses(List<String> value) {
            errorClasses = List.copyOf(value);
            return this;
        }

        public CircuitOpenOptionsBuilder halfOpenMaxProbes(int value) {
            halfOpenMaxProbes = value;
            return this;
        }

        public CircuitOpenOptionsBuilder halfOpenSuccessThreshold(int value) {
            halfOpenSuccessThreshold = value;
            return this;
        }

        public CircuitOpenOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public CircuitOpenOptions build() {
            return new CircuitOpenOptions(
                    openMs,
                    failureThreshold,
                    windowMs,
                    minCalls,
                    failureRatePct,
                    latencyThresholdMs,
                    errorClasses,
                    halfOpenMaxProbes,
                    halfOpenSuccessThreshold,
                    nowMs);
        }
    }

    public record BudgetReserveOptions(
            Long limit, Long windowMs, String reservationId, Long nowMs) {
        public BudgetReserveOptions {
            FlowValidation.requireOptionalPositive(limit, "limit");
            FlowValidation.requireOptionalPositive(windowMs, "windowMs");
            if (reservationId != null) {
                FlowValidation.requireText(reservationId, "reservationId");
            }
            FlowValidation.requireOptionalNonNegative(nowMs, "nowMs");
        }

        public static BudgetReserveOptionsBuilder builder() {
            return new BudgetReserveOptionsBuilder();
        }
    }

    public static final class BudgetReserveOptionsBuilder {
        private Long limit;
        private Long windowMs;
        private String reservationId;
        private Long nowMs;

        private BudgetReserveOptionsBuilder() {}

        public BudgetReserveOptionsBuilder limit(long value) {
            limit = value;
            return this;
        }

        public BudgetReserveOptionsBuilder windowMs(long value) {
            windowMs = value;
            return this;
        }

        public BudgetReserveOptionsBuilder reservationId(String value) {
            reservationId = value;
            return this;
        }

        public BudgetReserveOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public BudgetReserveOptions build() {
            return new BudgetReserveOptions(limit, windowMs, reservationId, nowMs);
        }
    }

    public record FilterOptions(String scope, String partitionKey, Integer limit, Long nowMs) {
        public FilterOptions {
            if (scope != null) {
                FlowValidation.requireText(scope, "scope");
            } else if (partitionKey != null) {
                FlowValidation.requireText(partitionKey, "partitionKey");
            }
            if (limit != null) {
                FlowValidation.requirePositive(limit, "limit");
            }
            FlowValidation.requireOptionalExactNonNegative(nowMs, "nowMs");
        }

        public static FilterOptionsBuilder builder() {
            return new FilterOptionsBuilder();
        }
    }

    public static final class FilterOptionsBuilder {
        private String scope;
        private String partitionKey;
        private Integer limit;
        private Long nowMs;

        private FilterOptionsBuilder() {}

        public FilterOptionsBuilder scope(String value) {
            scope = value;
            return this;
        }

        public FilterOptionsBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public FilterOptionsBuilder limit(int value) {
            limit = value;
            return this;
        }

        public FilterOptionsBuilder nowMs(long value) {
            nowMs = value;
            return this;
        }

        public FilterOptions build() {
            return new FilterOptions(scope, partitionKey, limit, nowMs);
        }
    }
}
