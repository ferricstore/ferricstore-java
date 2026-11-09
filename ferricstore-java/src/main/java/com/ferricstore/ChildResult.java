package com.ferricstore;

/**
 * One child of a group: its status and, once terminal, its decoded result or error.
 *
 * @param status {@code "running"}, {@code "completed"}, {@code "failed"}, or {@code "cancelled"}
 */
public record ChildResult(
        String id, String partitionKey, String status, Object result, Object error) {}
