package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Executes FerricStore commands independently of the public command builders. */
@FunctionalInterface
public interface CommandExecutor {
    Object execute(List<Object> args);

    /**
     * Executes a versioned Flow query.
     *
     * <p>Transports with a typed query operation should override this method. The default keeps
     * custom and test executors source-compatible by using the textual command envelope.
     */
    default Object flowQuery(String query, Map<String, ?> params) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(params, "params");
        List<Object> command = new ArrayList<>(3 + params.size() * 2);
        command.add("FLOW.QUERY");
        command.add("FQL1");
        command.add(query);
        params.forEach(
                (name, value) -> {
                    command.add(Objects.requireNonNull(name, "query parameter name"));
                    command.add(Objects.requireNonNull(value, "query parameter value"));
                });
        return execute(command);
    }

    default List<Object> pipeline(List<List<Object>> commands) {
        return commands.stream().map(this::execute).toList();
    }
}
