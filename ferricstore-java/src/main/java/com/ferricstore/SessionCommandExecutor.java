package com.ferricstore;

import java.time.Duration;

/** Dedicated native connection used by connection-affine command sessions. */
public interface SessionCommandExecutor extends CommandExecutor, AutoCloseable {
    default Object pollEvent(Duration timeout) {
        throw new UnsupportedOperationException("this session does not expose pushed events");
    }

    @Override
    void close();
}
