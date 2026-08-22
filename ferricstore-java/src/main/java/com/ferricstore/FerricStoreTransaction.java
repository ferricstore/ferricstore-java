package com.ferricstore;

import java.util.List;
import java.util.Objects;

/** One-use transaction bound to one owned native TCP connection. */
public final class FerricStoreTransaction implements AutoCloseable {
    private final SessionCommandExecutor executor;
    private final FerricStoreClient client;
    private boolean active = true;

    FerricStoreTransaction(SessionExecutorFactory factory, Codec codec, List<String> watchKeys) {
        executor = Objects.requireNonNull(factory.openSession(), "session executor");
        client = FerricStoreClient.fromExecutor(executor, codec);
        try {
            if (!watchKeys.isEmpty()) {
                HashSlot.requireSame("WATCH", watchKeys);
                List<Object> watch = CommandArgs.args("WATCH");
                watch.addAll(watchKeys);
                executor.execute(watch);
            }
            Object response = executor.execute(List.of("MULTI"));
            if (!CommandArgs.ok(response)) {
                throw new FerricStoreException("MULTI response must be OK");
            }
        } catch (RuntimeException error) {
            executor.close();
            throw error;
        }
    }

    /** Typed client backed by the isolated transaction connection. */
    public FerricStoreClient client() {
        requireActive();
        return client;
    }

    /** Queues one raw command on this transaction's isolated native connection. */
    public FerricStoreTransaction command(Object... args) {
        return command(List.of(args));
    }

    /** Queues one raw command on this transaction's isolated native connection. */
    public FerricStoreTransaction command(List<Object> args) {
        requireActive();
        Object response = executor.execute(args);
        if (!"QUEUED".equalsIgnoreCase(Resp.string(response))) {
            throw new FerricStoreException("transaction command response must be QUEUED");
        }
        return this;
    }

    /** Executes queued commands and permanently closes the transaction connection. */
    @SuppressWarnings("PMD.UseTryWithResources")
    public List<Object> execute() {
        requireActive();
        active = false;
        try {
            return Resp.list(executor.execute(List.of("EXEC")));
        } finally {
            executor.close();
        }
    }

    /** Discards queued commands and permanently closes the transaction connection. */
    @SuppressWarnings("PMD.UseTryWithResources")
    public void discard() {
        requireActive();
        active = false;
        try {
            Object response = executor.execute(List.of("DISCARD"));
            if (!CommandArgs.ok(response)) {
                throw new FerricStoreException("DISCARD response must be OK");
            }
        } finally {
            executor.close();
        }
    }

    @Override
    public void close() {
        if (active) {
            discard();
        }
    }

    private void requireActive() {
        if (!active) {
            throw new IllegalStateException("transaction is no longer active");
        }
    }
}
