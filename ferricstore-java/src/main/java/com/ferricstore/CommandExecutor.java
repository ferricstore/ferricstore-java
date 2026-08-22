package com.ferricstore;

import java.util.List;

/** Executes FerricStore commands independently of the public command builders. */
@FunctionalInterface
public interface CommandExecutor {
    Object execute(List<Object> args);

    default List<Object> pipeline(List<List<Object>> commands) {
        return commands.stream().map(this::execute).toList();
    }
}
