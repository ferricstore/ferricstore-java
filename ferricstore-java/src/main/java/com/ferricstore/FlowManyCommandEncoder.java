package com.ferricstore;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Strict parser for typed many-item Flow mutation transport payloads. */
final class FlowManyCommandEncoder {
    private static final Set<String> MAP_OPTIONS =
            Set.of("VALUE", "VALUE_REF", "ATTRIBUTE_MERGE", "STATE_META");
    private static final Set<String> LIST_OPTIONS =
            Set.of("DROP_VALUE", "OVERRIDE_VALUE", "ATTRIBUTE_DELETE");
    private static final Set<String> SCALAR_OPTIONS =
            Set.of(
                    "RESULT",
                    "ERROR",
                    "REASON",
                    "PAYLOAD",
                    "TTL",
                    "NOW",
                    "RUN_AT",
                    "PRIORITY",
                    "INDEPENDENT",
                    "RETURN");

    private FlowManyCommandEncoder() {}

    static Prepared tryPrepare(String normalizedName, List<Object> arguments) {
        FlowCommand command = FlowCommand.fromWireName(normalizedName).orElse(null);
        if (command == FlowCommand.COMPLETE_MANY) {
            return completeMany(command, arguments);
        }
        if (command == FlowCommand.TRANSITION_MANY) {
            return transitionMany(command, arguments);
        }
        if (command == FlowCommand.FAIL_MANY) {
            return claimedMany(command, arguments);
        }
        if (command == FlowCommand.CANCEL_MANY) {
            return cancelMany(command, arguments);
        }
        return null;
    }

    private static Prepared completeMany(FlowCommand command, List<Object> arguments) {
        return claimedMany(command, arguments);
    }

    private static Prepared claimedMany(FlowCommand command, List<Object> arguments) {
        if (arguments == null || arguments.size() < 3) {
            return null;
        }
        boolean mixed = mixed(arguments.get(0));
        Map<String, Object> payload = new LinkedHashMap<>();
        if (!mixed) {
            payload.put("partition_key", arguments.get(0));
        }
        int itemsAt = parseOptions(arguments, 1, payload);
        if (itemsAt < 0) {
            return null;
        }
        List<Object> items = claimedItems(arguments.subList(itemsAt, arguments.size()), mixed);
        if (items == null || items.isEmpty()) {
            return null;
        }
        payload.put("items", items);
        return prepared(command, payload);
    }

    private static Prepared cancelMany(FlowCommand command, List<Object> arguments) {
        if (arguments == null || arguments.size() < 3) {
            return null;
        }
        boolean mixed = mixed(arguments.get(0));
        Map<String, Object> payload = new LinkedHashMap<>();
        if (!mixed) {
            payload.put("partition_key", arguments.get(0));
        }
        int itemsAt = parseOptions(arguments, 1, payload);
        if (itemsAt < 0) {
            return null;
        }
        List<Object> items = fencedItems(arguments.subList(itemsAt, arguments.size()), mixed);
        if (items == null || items.isEmpty()) {
            return null;
        }
        payload.put("items", items);
        return prepared(command, payload);
    }

    private static Prepared transitionMany(FlowCommand command, List<Object> arguments) {
        if (arguments == null || arguments.size() < 5) {
            return null;
        }
        boolean mixed = mixed(arguments.get(0));
        Map<String, Object> payload = new LinkedHashMap<>();
        if (!mixed) {
            payload.put("partition_key", arguments.get(0));
        }
        payload.put("from_state", arguments.get(1));
        payload.put("to_state", arguments.get(2));
        int itemsAt = parseOptions(arguments, 3, payload);
        if (itemsAt < 0) {
            return null;
        }
        List<Object> items = transitionItems(arguments.subList(itemsAt, arguments.size()), mixed);
        if (items == null || items.isEmpty()) {
            return null;
        }
        payload.put("items", items);
        return prepared(command, payload);
    }

    private static int parseOptions(
            List<Object> arguments, int start, Map<String, Object> payload) {
        int index = start;
        while (index < arguments.size()) {
            String option = token(arguments.get(index));
            if ("ITEMS".equals(option)) {
                return index + 1;
            }
            if (option == null) {
                return -1;
            }
            if (MAP_OPTIONS.contains(option)) {
                if (index + 2 >= arguments.size()) {
                    return -1;
                }
                String field = mapField(option);
                Map<String, Object> values = mutableMap(payload, field);
                if (values == null) {
                    return -1;
                }
                String name = text(arguments.get(index + 1));
                if (name == null || values.putIfAbsent(name, arguments.get(index + 2)) != null) {
                    return -1;
                }
                index += 3;
                continue;
            }
            if (LIST_OPTIONS.contains(option)) {
                if (index + 1 >= arguments.size()) {
                    return -1;
                }
                String field = listField(option);
                List<Object> values = mutableList(payload, field);
                if (values == null) {
                    return -1;
                }
                values.add(arguments.get(index + 1));
                index += 2;
                continue;
            }
            if (!SCALAR_OPTIONS.contains(option) || index + 1 >= arguments.size()) {
                return -1;
            }
            String field = scalarField(option);
            if (payload.containsKey(field)) {
                return -1;
            }
            Object value = arguments.get(index + 1);
            if ("INDEPENDENT".equals(option)) {
                value = booleanValue(value);
                if (value == null) {
                    return -1;
                }
            } else if ("RETURN".equals(option)) {
                String returnMode = text(value);
                if (returnMode == null) {
                    return -1;
                }
                value = returnMode.toLowerCase(Locale.ROOT);
            }
            payload.put(field, value);
            index += 2;
        }
        return -1;
    }

    private static List<Object> claimedItems(List<Object> values, boolean mixed) {
        int width = mixed ? 4 : 3;
        if (values.isEmpty() || values.size() % width != 0) {
            return null;
        }
        List<Object> items = new ArrayList<>(values.size() / width);
        for (int index = 0; index < values.size(); index += width) {
            items.add(List.copyOf(values.subList(index, index + width)));
        }
        return List.copyOf(items);
    }

    private static List<Object> fencedItems(List<Object> values, boolean mixed) {
        int width = mixed ? 3 : 2;
        if (values.isEmpty() || values.size() % width != 0) {
            return null;
        }
        List<Object> items = new ArrayList<>(values.size() / width);
        for (int index = 0; index < values.size(); index += width) {
            items.add(List.copyOf(values.subList(index, index + width)));
        }
        return List.copyOf(items);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static List<Object> transitionItems(List<Object> values, boolean mixed) {
        int width = mixed ? 4 : 3;
        if (values.isEmpty() || values.size() % width != 0) {
            return null;
        }
        List<Object> items = new ArrayList<>(values.size() / width);
        for (int index = 0; index < values.size(); index += width) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", values.get(index));
            int fencingAt;
            int leaseAt;
            if (mixed) {
                item.put("partition_key", values.get(index + 1));
                fencingAt = index + 2;
                leaseAt = index + 3;
            } else {
                fencingAt = index + 1;
                leaseAt = index + 2;
            }
            item.put("fencing_token", values.get(fencingAt));
            Object lease = values.get(leaseAt);
            if (!dash(lease)) {
                item.put("lease_token", lease);
            }
            items.add(Map.copyOf(item));
        }
        return List.copyOf(items);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutableMap(Map<String, Object> payload, String field) {
        Object existing = payload.get(field);
        if (existing == null) {
            Map<String, Object> created = new LinkedHashMap<>();
            payload.put(field, created);
            return created;
        }
        return existing instanceof Map<?, ?> ? (Map<String, Object>) existing : null;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> mutableList(Map<String, Object> payload, String field) {
        Object existing = payload.get(field);
        if (existing == null) {
            List<Object> created = new ArrayList<>();
            payload.put(field, created);
            return created;
        }
        return existing instanceof List<?> ? (List<Object>) existing : null;
    }

    private static String mapField(String option) {
        return switch (option) {
            case "VALUE" -> "values";
            case "VALUE_REF" -> "value_refs";
            case "ATTRIBUTE_MERGE" -> "attributes_merge";
            default -> "state_meta";
        };
    }

    private static String listField(String option) {
        return switch (option) {
            case "DROP_VALUE" -> "drop_values";
            case "OVERRIDE_VALUE" -> "override_values";
            default -> "attributes_delete";
        };
    }

    private static String scalarField(String option) {
        return switch (option) {
            case "TTL" -> "ttl_ms";
            case "NOW" -> "now_ms";
            case "RUN_AT" -> "run_at_ms";
            default -> option.toLowerCase(Locale.ROOT);
        };
    }

    private static Object booleanValue(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        String normalized = token(value);
        if ("TRUE".equals(normalized)) {
            return true;
        }
        return "FALSE".equals(normalized) ? false : null;
    }

    private static boolean mixed(Object value) {
        return "MIXED".equals(token(value));
    }

    private static boolean dash(Object value) {
        return "-".equals(text(value));
    }

    private static String token(Object value) {
        String valueText = text(value);
        return valueText == null ? null : valueText.toUpperCase(Locale.ROOT);
    }

    private static String text(Object value) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        if (value instanceof byte[] bytes && bytes.length > 0) {
            try {
                return StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes))
                        .toString();
            } catch (CharacterCodingException ignored) {
                return null;
            }
        }
        return null;
    }

    private static Prepared prepared(FlowCommand command, Map<String, Object> payload) {
        return new Prepared(command, command.nativeOpcode().orElseThrow(), Map.copyOf(payload));
    }

    record Prepared(FlowCommand command, int opcode, Map<String, Object> payload) {}
}
