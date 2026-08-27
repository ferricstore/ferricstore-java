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

/** Encodes extended create-many and spawn-children items as typed native payloads. */
final class FlowItemCommandEncoder {
    private static final List<String> ITEM_MARKERS = List.of("ITEMS", "ITEMS_EXT", "ITEMS_MAPS");
    private static final List<String> MAP_OPTIONS =
            List.of("VALUE", "VALUE_REF", "ATTRIBUTE", "ATTRIBUTE_MERGE", "STATE_META");

    private FlowItemCommandEncoder() {}

    static Prepared tryPrepare(String normalizedName, List<Object> arguments) {
        FlowCommand command = FlowCommand.fromWireName(normalizedName).orElse(null);
        if (command == FlowCommand.CREATE_MANY) {
            return createMany(command, arguments);
        }
        if (command == FlowCommand.SPAWN_CHILDREN) {
            return spawnChildren(command, arguments);
        }
        return null;
    }

    private static Prepared createMany(FlowCommand command, List<Object> arguments) {
        if (arguments == null || arguments.size() < 3) {
            return null;
        }
        int markerAt = markerIndex(arguments, 1);
        if (markerAt < 0) {
            return null;
        }
        String partition = text(arguments.get(0));
        if (partition == null) {
            return null;
        }
        Map<String, Object> payload =
                new LinkedHashMap<>(
                        FlowCommandEncoder.optionPayload(command, arguments.subList(1, markerAt)));
        boolean mixed = "MIXED".equalsIgnoreCase(partition);
        if (!mixed && !"AUTO".equalsIgnoreCase(partition)) {
            payload.put("partition_key", arguments.get(0));
        }
        List<Map<String, Object>> items =
                parseCreateItems(
                        marker(arguments.get(markerAt)),
                        arguments.subList(markerAt + 1, arguments.size()),
                        mixed);
        if (items == null || items.isEmpty()) {
            return null;
        }
        payload.put("items", items);
        return prepared(command, payload);
    }

    private static Prepared spawnChildren(FlowCommand command, List<Object> arguments) {
        if (arguments == null || arguments.size() < 3) {
            return null;
        }
        int markerAt = markerIndex(arguments, 1);
        if (markerAt < 0) {
            return null;
        }
        Map<String, Object> payload =
                new LinkedHashMap<>(
                        FlowCommandEncoder.optionPayload(command, arguments.subList(1, markerAt)));
        payload.put("id", arguments.get(0));
        List<Object> itemArguments = arguments.subList(markerAt + 1, arguments.size());
        String itemMarker = marker(arguments.get(markerAt));
        List<Map<String, Object>> children =
                "ITEMS".equals(itemMarker)
                        ? parseSimpleChildren(itemArguments)
                        : parseMappedOrExtendedChildren(itemMarker, itemArguments);
        if (children == null || children.isEmpty()) {
            return null;
        }
        payload.put("children", children);
        return prepared(command, payload);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // One map is required per item.
    private static List<Map<String, Object>> parseCreateItems(
            String itemMarker, List<Object> values, boolean mixed) {
        if ("ITEMS_MAPS".equals(itemMarker)) {
            return mappedItems(values);
        }
        if ("ITEMS_EXT".equals(itemMarker)) {
            return extendedCreateItems(values);
        }
        int width = mixed ? 3 : 2;
        if (!"ITEMS".equals(itemMarker) || values.isEmpty() || values.size() % width != 0) {
            return null;
        }
        List<Map<String, Object>> items = new ArrayList<>(values.size() / width);
        for (int index = 0; index < values.size(); index += width) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", values.get(index));
            int payloadAt = index + 1;
            if (mixed) {
                putPartition(item, values.get(index + 1));
                payloadAt++;
            }
            item.put("payload", values.get(payloadAt));
            items.add(Map.copyOf(item));
        }
        return List.copyOf(items);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // One map is required per item.
    private static List<Map<String, Object>> extendedCreateItems(List<Object> values) {
        CountedValues counted = counted(values);
        if (counted == null) {
            return null;
        }
        List<Map<String, Object>> items = new ArrayList<>(counted.count());
        int index = 0;
        for (int itemIndex = 0; itemIndex < counted.count(); itemIndex++) {
            if (index + 3 > counted.values().size()) {
                return null;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", counted.values().get(index++));
            putPartition(item, counted.values().get(index++));
            item.put("payload", counted.values().get(index++));
            index = appendNamedMaps(counted.values(), index, item);
            if (index < 0) {
                return null;
            }
            items.add(Map.copyOf(item));
        }
        return index == counted.values().size() ? List.copyOf(items) : null;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // One map is required per child.
    private static List<Map<String, Object>> parseSimpleChildren(List<Object> values) {
        int width = 3;
        if (values.isEmpty() || values.size() % width != 0) {
            return null;
        }
        List<Map<String, Object>> children = new ArrayList<>(values.size() / width);
        for (int index = 0; index < values.size(); index += width) {
            Map<String, Object> child = new LinkedHashMap<>();
            child.put("id", values.get(index));
            child.put("type", values.get(index + 1));
            child.put("payload", values.get(index + 2));
            children.add(Map.copyOf(child));
        }
        return List.copyOf(children);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // One map is required per child.
    private static List<Map<String, Object>> parseMappedOrExtendedChildren(
            String itemMarker, List<Object> values) {
        if ("ITEMS_MAPS".equals(itemMarker)) {
            return mappedItems(values);
        }
        if (!"ITEMS_EXT".equals(itemMarker)) {
            return null;
        }
        CountedValues counted = counted(values);
        if (counted == null) {
            return null;
        }
        List<Map<String, Object>> children = new ArrayList<>(counted.count());
        int index = 0;
        for (int itemIndex = 0; itemIndex < counted.count(); itemIndex++) {
            if (index + 4 > counted.values().size()) {
                return null;
            }
            Map<String, Object> child = new LinkedHashMap<>();
            child.put("id", counted.values().get(index++));
            putPartition(child, counted.values().get(index++));
            child.put("type", counted.values().get(index++));
            child.put("payload", counted.values().get(index++));
            index = appendNamedMaps(counted.values(), index, child);
            if (index < 0) {
                return null;
            }
            children.add(Map.copyOf(child));
        }
        return index == counted.values().size() ? List.copyOf(children) : null;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") // Defensive copy per item.
    private static List<Map<String, Object>> mappedItems(List<Object> values) {
        CountedValues counted = counted(values);
        if (counted == null || counted.values().size() != counted.count()) {
            return null;
        }
        List<Map<String, Object>> items = new ArrayList<>(counted.count());
        for (Object value : counted.values()) {
            if (!(value instanceof Map<?, ?> map)) {
                return null;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = text(entry.getKey());
                if (key == null) {
                    return null;
                }
                item.put(key, entry.getValue());
            }
            items.add(Map.copyOf(item));
        }
        return List.copyOf(items);
    }

    private static int appendNamedMaps(List<Object> values, int start, Map<String, Object> item) {
        NamedMap inline = namedMap(values, start);
        if (inline == null) {
            return -1;
        }
        NamedMap refs = namedMap(values, inline.next());
        if (refs == null) {
            return -1;
        }
        if (!inline.values().isEmpty()) {
            item.put("values", inline.values());
        }
        if (!refs.values().isEmpty()) {
            item.put("value_refs", refs.values());
        }
        return refs.next();
    }

    private static NamedMap namedMap(List<Object> values, int start) {
        if (start >= values.size() || !(values.get(start) instanceof Number number)) {
            return null;
        }
        int count = number.intValue();
        if (count < 0 || start + 1L + count * 2L > values.size()) {
            return null;
        }
        Map<String, Object> mapped = new LinkedHashMap<>();
        int index = start + 1;
        for (int item = 0; item < count; item++) {
            String name = text(values.get(index++));
            if (name == null || mapped.putIfAbsent(name, values.get(index++)) != null) {
                return null;
            }
        }
        return new NamedMap(Map.copyOf(mapped), index);
    }

    private static CountedValues counted(List<Object> values) {
        if (values.isEmpty() || !(values.get(0) instanceof Number number)) {
            return null;
        }
        int count = number.intValue();
        return count > 0 ? new CountedValues(count, values.subList(1, values.size())) : null;
    }

    private static int markerIndex(List<Object> arguments, int start) {
        int index = start;
        while (index < arguments.size()) {
            String itemMarker = marker(arguments.get(index));
            if (itemMarker != null && ITEM_MARKERS.contains(itemMarker)) {
                return index;
            }
            if (itemMarker == null) {
                return -1;
            }
            index += MAP_OPTIONS.contains(itemMarker) ? 3 : 2;
        }
        return -1;
    }

    private static String marker(Object value) {
        String valueText = text(value);
        return valueText == null ? null : valueText.toUpperCase(Locale.ROOT);
    }

    private static void putPartition(Map<String, Object> item, Object value) {
        if (!"-".equals(text(value))) {
            item.put("partition_key", value);
        }
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

    private record NamedMap(Map<String, Object> values, int next) {}

    private record CountedValues(int count, List<Object> values) {}

    record Prepared(FlowCommand command, int opcode, Map<String, Object> payload) {}
}
