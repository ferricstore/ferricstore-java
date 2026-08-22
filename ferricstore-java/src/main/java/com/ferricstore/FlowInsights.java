package com.ferricstore;

import static com.ferricstore.CommandArgs.append;
import static com.ferricstore.CommandArgs.appendBool;
import static com.ferricstore.CommandArgs.args;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Projected Flow statistics, indexed attributes, and query-index diagnostics. */
public final class FlowInsights {
    private final CommandExecutor executor;

    FlowInsights(CommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public Map<String, Object> stats(String type, StatsOptions options) {
        FlowValidation.requireText(type, "flow type");
        StatsOptions effective = options == null ? StatsOptions.builder().build() : options;
        List<Object> command = args(FlowCommand.STATS.wireName(), type);
        appendReadOptions(command, effective.read());
        effective
                .attributes()
                .forEach(
                        (name, value) -> {
                            command.add("ATTRIBUTE");
                            command.add(name);
                            command.add(value);
                        });
        return Resp.parseKv(executor.execute(command));
    }

    public List<Map<String, Object>> attributes(String type, ReadOptions options) {
        FlowValidation.requireText(type, "flow type");
        List<Object> command = args(FlowCommand.ATTRIBUTES.wireName(), type);
        appendReadOptions(command, options == null ? ReadOptions.builder().build() : options);
        return Resp.maps(executor.execute(command));
    }

    public List<Map<String, Object>> attributeValues(
            String type, String attribute, ReadOptions options) {
        FlowValidation.requireText(type, "flow type");
        FlowValidation.requireText(attribute, "flow attribute");
        List<Object> command = args(FlowCommand.ATTRIBUTE_VALUES.wireName(), type, attribute);
        appendReadOptions(command, options == null ? ReadOptions.builder().build() : options);
        return Resp.maps(executor.execute(command));
    }

    public Map<String, Object> queryIndexes() {
        return queryIndexes(null);
    }

    public Map<String, Object> queryIndexes(String indexId) {
        List<Object> command = args(FlowCommand.QUERY_INDEXES.wireName());
        if (indexId != null) {
            FlowValidation.requireText(indexId, "query index id");
            if (indexId.length() > 128 || !indexId.matches("[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")) {
                throw new IllegalArgumentException("query index id has an invalid format");
            }
            command.add(indexId);
        }
        return Resp.parseKv(executor.execute(command));
    }

    private static void appendReadOptions(List<Object> command, ReadOptions options) {
        append(command, "STATE", options.state());
        append(command, "PARTITION", options.partitionKey());
        append(command, "COUNT", options.count());
        appendBool(command, "CONSISTENT_PROJECTION", options.consistentProjection());
    }

    public record ReadOptions(
            String state, String partitionKey, Integer count, Boolean consistentProjection) {
        public ReadOptions {
            if (state != null) {
                FlowValidation.requireText(state, "state");
            }
            if (partitionKey != null) {
                FlowValidation.requireText(partitionKey, "partition key");
            }
            if (count != null) {
                FlowValidation.requirePositive(count, "count");
            }
        }

        public static ReadOptionsBuilder builder() {
            return new ReadOptionsBuilder();
        }
    }

    public static final class ReadOptionsBuilder {
        private String state;
        private String partitionKey;
        private Integer count;
        private Boolean consistentProjection;

        private ReadOptionsBuilder() {}

        public ReadOptionsBuilder state(String value) {
            state = value;
            return this;
        }

        public ReadOptionsBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public ReadOptionsBuilder count(int value) {
            count = value;
            return this;
        }

        public ReadOptionsBuilder consistentProjection(boolean value) {
            consistentProjection = value;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(state, partitionKey, count, consistentProjection);
        }
    }

    public record StatsOptions(ReadOptions read, Map<String, ?> attributes) {
        public StatsOptions {
            read = read == null ? ReadOptions.builder().build() : read;
            attributes = ImmutableCopies.map(attributes);
        }

        public static StatsOptionsBuilder builder() {
            return new StatsOptionsBuilder();
        }
    }

    public static final class StatsOptionsBuilder {
        private String state;
        private String partitionKey;
        private Integer count;
        private Boolean consistentProjection;
        private final Map<String, Object> attributes = new LinkedHashMap<>();

        private StatsOptionsBuilder() {}

        public StatsOptionsBuilder state(String value) {
            state = value;
            return this;
        }

        public StatsOptionsBuilder partitionKey(String value) {
            partitionKey = value;
            return this;
        }

        public StatsOptionsBuilder count(int value) {
            count = value;
            return this;
        }

        public StatsOptionsBuilder consistentProjection(boolean value) {
            consistentProjection = value;
            return this;
        }

        public StatsOptionsBuilder attribute(String name, Object value) {
            FlowValidation.requireText(name, "attribute name");
            attributes.put(name, value);
            return this;
        }

        public StatsOptions build() {
            return new StatsOptions(
                    new ReadOptions(state, partitionKey, count, consistentProjection), attributes);
        }
    }
}
