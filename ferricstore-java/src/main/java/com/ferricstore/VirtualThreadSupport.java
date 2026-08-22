package com.ferricstore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class VirtualThreadSupport {
    private static final Method NEW_VIRTUAL_THREAD_EXECUTOR = findFactory();

    private VirtualThreadSupport() {}

    static boolean isAvailable() {
        return NEW_VIRTUAL_THREAD_EXECUTOR != null;
    }

    static ExecutorService newExecutor() {
        Method factory = NEW_VIRTUAL_THREAD_EXECUTOR;
        if (factory == null) {
            throw new UnsupportedOperationException(
                    "Virtual threads require Java 21 or newer; current runtime is Java "
                            + Runtime.version().feature());
        }
        try {
            return (ExecutorService) factory.invoke(null);
        } catch (IllegalAccessException error) {
            throw new IllegalStateException(
                    "Virtual-thread executor factory is inaccessible", error);
        } catch (InvocationTargetException error) {
            throw new IllegalStateException(
                    "Virtual-thread executor creation failed", error.getCause());
        }
    }

    private static Method findFactory() {
        if (Runtime.version().feature() < 21) {
            return null;
        }
        try {
            return Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        } catch (NoSuchMethodException | SecurityException ignored) {
            return null;
        }
    }
}
