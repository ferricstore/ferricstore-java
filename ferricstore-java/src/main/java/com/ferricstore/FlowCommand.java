package com.ferricstore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

/** FerricStore 0.11.8 Flow command catalog shared by every transport. */
public enum FlowCommand {
    CREATE("FLOW.CREATE", 0x0201),
    GET("FLOW.GET", 0x0202),
    CLAIM_DUE("FLOW.CLAIM_DUE", 0x0203),
    COMPLETE("FLOW.COMPLETE", 0x0204),
    TRANSITION("FLOW.TRANSITION", 0x0205),
    RETRY("FLOW.RETRY", 0x0206),
    FAIL("FLOW.FAIL", 0x0207),
    CANCEL("FLOW.CANCEL", 0x0208),
    EXTEND_LEASE("FLOW.EXTEND_LEASE", 0x0209),
    HISTORY("FLOW.HISTORY", 0x020A),
    VALUE_PUT("FLOW.VALUE.PUT", 0x020B),
    VALUE_MGET("FLOW.VALUE.MGET", 0x020C),
    SIGNAL("FLOW.SIGNAL", 0x020D),
    CREATE_MANY("FLOW.CREATE_MANY", 0x020F),
    COMPLETE_MANY("FLOW.COMPLETE_MANY", 0x0210),
    TRANSITION_MANY("FLOW.TRANSITION_MANY", 0x0211),
    RETRY_MANY("FLOW.RETRY_MANY", 0x0212),
    FAIL_MANY("FLOW.FAIL_MANY", 0x0213),
    CANCEL_MANY("FLOW.CANCEL_MANY", 0x0214),
    RECLAIM("FLOW.RECLAIM", 0x0215),
    REWIND("FLOW.REWIND", 0x0216),
    INFO("FLOW.INFO", 0x021C),
    POLICY_SET("FLOW.POLICY.SET", 0x021E),
    POLICY_GET("FLOW.POLICY.GET", 0x021F),
    SPAWN_CHILDREN("FLOW.SPAWN_CHILDREN", 0x0220),
    RETENTION_CLEANUP("FLOW.RETENTION_CLEANUP", 0x0221),
    STEP_CONTINUE("FLOW.STEP_CONTINUE", 0x0222),
    START_AND_CLAIM("FLOW.START_AND_CLAIM", 0x0223),
    RUN_STEPS_MANY("FLOW.RUN_STEPS_MANY", 0x0224),
    SCHEDULE_CREATE("FLOW.SCHEDULE.CREATE", 0x0225),
    SCHEDULE_GET("FLOW.SCHEDULE.GET", 0x0226),
    SCHEDULE_DELETE("FLOW.SCHEDULE.DELETE", 0x0227),
    SCHEDULE_FIRE_DUE("FLOW.SCHEDULE.FIRE_DUE", 0x0228),
    SCHEDULE_LIST("FLOW.SCHEDULE.LIST", 0x0229),
    SCHEDULE_FIRE("FLOW.SCHEDULE.FIRE", 0x022A),
    SCHEDULE_PAUSE("FLOW.SCHEDULE.PAUSE", 0x022B),
    SCHEDULE_RESUME("FLOW.SCHEDULE.RESUME", 0x022C),
    STATS("FLOW.STATS", 0x022D),
    ATTRIBUTES("FLOW.ATTRIBUTES", 0x022E),
    ATTRIBUTE_VALUES("FLOW.ATTRIBUTE_VALUES", 0x022F),
    QUERY("FLOW.QUERY", 0x0231),
    EFFECT_RESERVE("FLOW.EFFECT.RESERVE", 0x0240),
    EFFECT_CONFIRM("FLOW.EFFECT.CONFIRM", 0x0241),
    EFFECT_FAIL("FLOW.EFFECT.FAIL", 0x0242),
    EFFECT_COMPENSATE("FLOW.EFFECT.COMPENSATE", 0x0243),
    EFFECT_GET("FLOW.EFFECT.GET", 0x0244),
    GOVERNANCE_LEDGER("FLOW.GOVERNANCE.LEDGER", 0x0245),
    APPROVAL_REQUEST("FLOW.APPROVAL.REQUEST", 0x0246),
    APPROVAL_APPROVE("FLOW.APPROVAL.APPROVE", 0x0247),
    APPROVAL_REJECT("FLOW.APPROVAL.REJECT", 0x0248),
    APPROVAL_GET("FLOW.APPROVAL.GET", 0x0249),
    CIRCUIT_OPEN("FLOW.CIRCUIT.OPEN", 0x024A),
    CIRCUIT_CLOSE("FLOW.CIRCUIT.CLOSE", 0x024B),
    CIRCUIT_GET("FLOW.CIRCUIT.GET", 0x024C),
    BUDGET_RESERVE("FLOW.BUDGET.RESERVE", 0x024D),
    BUDGET_GET("FLOW.BUDGET.GET", 0x024E),
    LIMIT_LEASE("FLOW.LIMIT.LEASE", 0x024F),
    LIMIT_SPEND("FLOW.LIMIT.SPEND", 0x0250),
    LIMIT_RELEASE("FLOW.LIMIT.RELEASE", 0x0251),
    LIMIT_GET("FLOW.LIMIT.GET", 0x0252),
    APPROVAL_LIST("FLOW.APPROVAL.LIST", 0x0253),
    GOVERNANCE_OVERVIEW("FLOW.GOVERNANCE.OVERVIEW", 0x0254),
    BUDGET_LIST("FLOW.BUDGET.LIST", 0x0255),
    LIMIT_LIST("FLOW.LIMIT.LIST", 0x0256),
    BUDGET_COMMIT("FLOW.BUDGET.COMMIT", 0x0257),
    BUDGET_RELEASE("FLOW.BUDGET.RELEASE", 0x0258),
    QUERY_INDEXES("FLOW.QUERY.INDEXES");

    private static final Map<String, FlowCommand> BY_WIRE_NAME =
            Arrays.stream(values())
                    .collect(
                            Collectors.toUnmodifiableMap(
                                    FlowCommand::wireName, Function.identity()));

    private final String wireName;
    private final Integer nativeOpcode;

    FlowCommand(String wireName, int nativeOpcode) {
        this.wireName = wireName;
        this.nativeOpcode = nativeOpcode;
    }

    FlowCommand(String wireName) {
        this.wireName = wireName;
        this.nativeOpcode = null;
    }

    public String wireName() {
        return wireName;
    }

    public OptionalInt nativeOpcode() {
        return nativeOpcode == null ? OptionalInt.empty() : OptionalInt.of(nativeOpcode);
    }

    public static Optional<FlowCommand> fromWireName(String value) {
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_WIRE_NAME.get(value.toUpperCase(Locale.ROOT)));
    }

    @Override
    public String toString() {
        return wireName;
    }
}
