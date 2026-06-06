package com.ferricstore;

import java.util.List;

@FunctionalInterface
public interface RedisExecutor {
    Object execute(List<Object> args);

    default List<Object> pipeline(List<List<Object>> commands) {
        return commands.stream().map(this::execute).toList();
    }
}
