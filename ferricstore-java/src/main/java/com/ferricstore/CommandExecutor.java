package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Executes FerricStore commands independently of the public command builders. */
@FunctionalInterface
public interface CommandExecutor {
    Object execute(List<Object> args);

    /**
     * Executes a command without requiring the caller to wait for its response.
     *
     * <p>Network transports override this method with true asynchronous I/O. The default preserves
     * source compatibility for lightweight custom and test executors.
     */
    default CompletableFuture<Object> executeAsync(List<Object> args) {
        try {
            return CompletableFuture.completedFuture(execute(args));
        } catch (RuntimeException error) {
            return AsyncFutures.failed(error);
        }
    }

    /**
     * Executes a versioned Flow query.
     *
     * <p>Transports with a typed query operation should override this method. The default keeps
     * custom and test executors source-compatible by using the textual command envelope.
     */
    default Object flowQuery(String query, Map<String, ?> params) {
        return AsyncFutures.await(
                flowQueryAsync(query, params), "Flow query was interrupted while waiting");
    }

    /** Executes a versioned Flow query without blocking for its response. */
    default CompletableFuture<Object> flowQueryAsync(String query, Map<String, ?> params) {
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
        return executeAsync(command);
    }

    default List<Object> pipeline(List<List<Object>> commands) {
        return commands.stream().map(this::execute).toList();
    }

    /** Executes independent commands concurrently and preserves their input order in the result. */
    default CompletableFuture<List<Object>> pipelineAsync(List<List<Object>> commands) {
        Objects.requireNonNull(commands, "commands");
        return AsyncFutures.sequence(commands.stream().map(this::executeAsync).toList());
    }
}
