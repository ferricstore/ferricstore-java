package com.ferricstore;

@FunctionalInterface
public interface QueueHandler {
    Object handle(FlowRecord job) throws Exception;
}
