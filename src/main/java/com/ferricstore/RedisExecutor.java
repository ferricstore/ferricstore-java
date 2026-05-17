package com.ferricstore;

import java.util.List;

@FunctionalInterface
public interface RedisExecutor {
    Object execute(List<Object> args);
}
