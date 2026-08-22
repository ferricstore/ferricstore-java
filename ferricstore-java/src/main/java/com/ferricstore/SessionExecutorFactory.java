package com.ferricstore;

/** Capability implemented by native TCP executors that can open isolated sessions. */
@FunctionalInterface
public interface SessionExecutorFactory {
    SessionCommandExecutor openSession();
}
