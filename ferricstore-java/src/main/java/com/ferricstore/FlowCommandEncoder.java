package com.ferricstore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Converts structured-only public Flow grammar into the shared native/HTTP payload contract. */
final class FlowCommandEncoder {
    private static final Set<FlowCommand> STRUCTURED_COMMANDS =
            Set.of(
                    FlowCommand.VALUE_MGET,
                    FlowCommand.COMPLETE_MANY,
                    FlowCommand.TRANSITION_MANY,
                    FlowCommand.STEP_CONTINUE,
                    FlowCommand.START_AND_CLAIM,
                    FlowCommand.RUN_STEPS_MANY,
                    FlowCommand.SCHEDULE_CREATE,
                    FlowCommand.SCHEDULE_GET,
                    FlowCommand.SCHEDULE_DELETE,
                    FlowCommand.SCHEDULE_FIRE_DUE,
                    FlowCommand.SCHEDULE_LIST,
                    FlowCommand.SCHEDULE_FIRE,
                    FlowCommand.SCHEDULE_PAUSE,
                    FlowCommand.SCHEDULE_RESUME,
                    FlowCommand.EFFECT_RESERVE,
                    FlowCommand.EFFECT_CONFIRM,
                    FlowCommand.EFFECT_FAIL,
                    FlowCommand.EFFECT_COMPENSATE,
                    FlowCommand.EFFECT_GET,
                    FlowCommand.GOVERNANCE_LEDGER,
                    FlowCommand.GOVERNANCE_OVERVIEW,
                    FlowCommand.APPROVAL_REQUEST,
                    FlowCommand.APPROVAL_APPROVE,
                    FlowCommand.APPROVAL_REJECT,
                    FlowCommand.APPROVAL_GET,
                    FlowCommand.APPROVAL_LIST,
                    FlowCommand.CIRCUIT_OPEN,
                    FlowCommand.CIRCUIT_CLOSE,
                    FlowCommand.CIRCUIT_GET,
                    FlowCommand.BUDGET_RESERVE,
                    FlowCommand.BUDGET_COMMIT,
                    FlowCommand.BUDGET_RELEASE,
                    FlowCommand.BUDGET_GET,
                    FlowCommand.BUDGET_LIST,
                    FlowCommand.LIMIT_LEASE,
                    FlowCommand.LIMIT_SPEND,
                    FlowCommand.LIMIT_RELEASE,
                    FlowCommand.LIMIT_GET,
                    FlowCommand.LIMIT_LIST);
    private static final Set<FlowCommand> ID_SCOPED =
            Set.of(
                    FlowCommand.SCHEDULE_CREATE,
                    FlowCommand.SCHEDULE_GET,
                    FlowCommand.SCHEDULE_DELETE,
                    FlowCommand.SCHEDULE_FIRE,
                    FlowCommand.SCHEDULE_PAUSE,
                    FlowCommand.SCHEDULE_RESUME,
                    FlowCommand.EFFECT_RESERVE,
                    FlowCommand.EFFECT_CONFIRM,
                    FlowCommand.EFFECT_FAIL,
                    FlowCommand.EFFECT_COMPENSATE,
                    FlowCommand.EFFECT_GET,
                    FlowCommand.GOVERNANCE_LEDGER,
                    FlowCommand.APPROVAL_REQUEST,
                    FlowCommand.APPROVAL_APPROVE,
                    FlowCommand.APPROVAL_REJECT,
                    FlowCommand.APPROVAL_GET);
    private static final Set<FlowCommand> SCOPE_SCOPED =
            Set.of(
                    FlowCommand.CIRCUIT_OPEN,
                    FlowCommand.CIRCUIT_CLOSE,
                    FlowCommand.CIRCUIT_GET,
                    FlowCommand.BUDGET_RESERVE,
                    FlowCommand.BUDGET_COMMIT,
                    FlowCommand.BUDGET_RELEASE,
                    FlowCommand.BUDGET_GET,
                    FlowCommand.LIMIT_LEASE,
                    FlowCommand.LIMIT_SPEND,
                    FlowCommand.LIMIT_RELEASE,
                    FlowCommand.LIMIT_GET);
    private static final Set<String> BOOLEAN_FIELDS =
            Set.of(
                    "consistent_projection",
                    "full",
                    "idempotent",
                    "include_cold",
                    "independent",
                    "local_cache",
                    "override",
                    "overwrite",
                    "reclaim_expired",
                    "replace",
                    "rev",
                    "terminal_only",
                    "terminal_local_only",
                    "values");
    private static final Set<String> MAP_ENTRY_OPTIONS =
            Set.of("VALUE", "VALUE_REF", "ATTRIBUTE", "ATTRIBUTE_MERGE", "STATE_META");
    private static final Set<String> LIST_ENTRY_OPTIONS =
            Set.of("DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_DELETE");

    private FlowCommandEncoder() {}

    static Prepared prepare(String normalizedName, List<Object> arguments) {
        FlowCommand command = FlowCommand.fromWireName(normalizedName).orElse(null);
        if (command == null || !STRUCTURED_COMMANDS.contains(command)) {
            return null;
        }
        int opcode =
                command.nativeOpcode()
                        .orElseThrow(
                                () ->
                                        new InvalidCommandException(
                                                command.wireName()
                                                        + " has no native structured opcode"));
        List<Object> args = List.copyOf(arguments);
        Map<String, Object> payload;
        if (command == FlowCommand.COMPLETE_MANY || command == FlowCommand.TRANSITION_MANY) {
            FlowManyCommandEncoder.Prepared many =
                    FlowManyCommandEncoder.tryPrepare(normalizedName, arguments);
            return many == null ? null : new Prepared(command, many.opcode(), many.payload());
        } else if (command == FlowCommand.VALUE_MGET) {
            payload = valueMget(args);
        } else if (command == FlowCommand.STEP_CONTINUE) {
            payload =
                    positional(
                            command, args, List.of("id", "lease_token", "from_state", "to_state"));
        } else if (command == FlowCommand.START_AND_CLAIM) {
            payload = positional(command, args, List.of("id"));
        } else if (ID_SCOPED.contains(command)) {
            payload = positional(command, args, List.of("id"));
        } else if (SCOPE_SCOPED.contains(command)) {
            payload = positional(command, args, List.of("scope"));
        } else {
            payload = options(command, args, 0);
        }
        return new Prepared(command, opcode, Map.copyOf(payload));
    }

    private static Map<String, Object> valueMget(List<Object> args) {
        int option = args.size();
        for (int index = 0; index < args.size(); index++) {
            if ("MAX_BYTES".equals(tokenOrNull(args.get(index)))) {
                option = index;
                break;
            }
        }
        if (option == 0) {
            throw new InvalidCommandException("FLOW.VALUE.MGET requires at least one ref");
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("refs", new ArrayList<>(args.subList(0, option)));
        if (option < args.size()) {
            if (option + 2 != args.size()) {
                throw malformed(FlowCommand.VALUE_MGET, "MAX_BYTES requires exactly one value");
            }
            payload.put("max_bytes", args.get(option + 1));
        }
        return payload;
    }

    private static Map<String, Object> positional(
            FlowCommand command, List<Object> args, List<String> names) {
        if (args.size() < names.size()) {
            throw malformed(command, "requires " + names.size() + " positional arguments");
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        for (int index = 0; index < names.size(); index++) {
            payload.put(names.get(index), args.get(index));
        }
        payload.putAll(options(command, args, names.size()));
        return payload;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> options(FlowCommand command, List<Object> args, int start) {
        Map<String, Object> payload = new LinkedHashMap<>();
        int index = start;
        while (index < args.size()) {
            String token = token(args.get(index), command);
            if (MAP_ENTRY_OPTIONS.contains(token)) {
                requireRemaining(command, args, index, 3, token);
                String field =
                        switch (token) {
                            case "VALUE" -> "values";
                            case "VALUE_REF" -> "value_refs";
                            case "ATTRIBUTE" -> "attributes";
                            case "ATTRIBUTE_MERGE" -> "attributes_merge";
                            default -> "state_meta";
                        };
                Map<String, Object> values = mapField(payload, field);
                values.put(Resp.string(args.get(index + 1)), args.get(index + 2));
                index += 3;
                continue;
            }
            if (LIST_ENTRY_OPTIONS.contains(token)) {
                requireRemaining(command, args, index, 2, token);
                String field =
                        switch (token) {
                            case "DROP_VALUE" -> "drop_values";
                            case "OVERRIDE_VALUE" -> "override_values";
                            default -> "attributes_delete";
                        };
                List<Object> values = listField(payload, field);
                values.add(args.get(index + 1));
                index += 2;
                continue;
            }
            if ("NOPAYLOAD".equals(token)) {
                payload.put("payload", false);
                index++;
                continue;
            }
            requireRemaining(command, args, index, 2, token);
            String field = fieldName(token);
            Object value = args.get(index + 1);
            if ("RETURN".equals(token)) {
                value = Resp.string(value).toLowerCase(Locale.ROOT);
            } else if (BOOLEAN_FIELDS.contains(field)) {
                value = booleanValue(value, command, token);
            }
            if ("STATE".equals(token) && payload.containsKey("state")) {
                Object previous = payload.remove("state");
                List<Object> states = mutableList(previous);
                states.add(value);
                payload.put("states", states);
            } else if ("STATE".equals(token) && payload.containsKey("states")) {
                ((List<Object>) payload.get("states")).add(value);
            } else if ("IF_STATE".equals(token) && payload.containsKey(field)) {
                Object previous = payload.get(field);
                List<Object> states = mutableList(previous);
                states.add(value);
                payload.put(field, states);
            } else {
                payload.put(field, value);
            }
            index += 2;
        }
        return payload;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapField(Map<String, Object> payload, String field) {
        return (Map<String, Object>)
                payload.computeIfAbsent(field, ignored -> new LinkedHashMap<>());
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listField(Map<String, Object> payload, String field) {
        return (List<Object>) payload.computeIfAbsent(field, ignored -> new ArrayList<>());
    }

    private static List<Object> mutableList(Object existing) {
        return existing instanceof List<?> list
                ? new ArrayList<>(list)
                : new ArrayList<>(List.of(existing));
    }

    private static String fieldName(String token) {
        return switch (token) {
            case "BLOCK" -> "block_ms";
            case "FENCING", "FENCING_TOKEN" -> "fencing_token";
            case "GROUP" -> "group_id";
            case "IDEMPOTENCY" -> "idempotency_key";
            case "MAXBYTES" -> "payload_max_bytes";
            case "NOW" -> "now_ms";
            case "PARTITION" -> "partition_key";
            case "RETENTION_TTL" -> "retention_ttl_ms";
            case "RUN_AT" -> "run_at_ms";
            case "TTL" -> "ttl_ms";
            default -> token.toLowerCase(Locale.ROOT);
        };
    }

    private static Object booleanValue(Object value, FlowCommand command, String token) {
        if (value instanceof Boolean) {
            return value;
        }
        if (value instanceof String text) {
            if ("true".equalsIgnoreCase(text)) {
                return true;
            }
            if ("false".equalsIgnoreCase(text)) {
                return false;
            }
        }
        throw malformed(command, token + " requires a boolean value");
    }

    private static String token(Object value, FlowCommand command) {
        String token = tokenOrNull(value);
        if (token == null || token.isBlank()) {
            throw malformed(command, "option names must be non-blank text");
        }
        return token;
    }

    private static String tokenOrNull(Object value) {
        if (value instanceof String text) {
            return text.toUpperCase(Locale.ROOT);
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
                    .toUpperCase(Locale.ROOT);
        }
        return null;
    }

    private static void requireRemaining(
            FlowCommand command, List<Object> args, int index, int width, String token) {
        if (index + width > args.size()) {
            throw malformed(command, token + " is missing required values");
        }
    }

    private static InvalidCommandException malformed(FlowCommand command, String message) {
        return new InvalidCommandException(command.wireName() + " " + message);
    }

    record Prepared(FlowCommand command, int opcode, Map<String, Object> payload) {
        Prepared {
            Objects.requireNonNull(command, "command");
            Objects.requireNonNull(payload, "payload");
        }
    }
}
