package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

final class VirtualThreadSupportTest {
    @Test
    void matchesTheCurrentRuntimeCapability() {
        assertEquals(Runtime.version().feature() >= 21, VirtualThreadSupport.isAvailable());
    }

    @Test
    void createsVirtualThreadsOrFailsClearly() throws Exception {
        if (Runtime.version().feature() < 21) {
            UnsupportedOperationException error =
                    assertThrows(
                            UnsupportedOperationException.class, VirtualThreadSupport::newExecutor);

            assertTrue(error.getMessage().contains("require Java 21 or newer"));
            return;
        }

        ExecutorService executor = VirtualThreadSupport.newExecutor();
        try {
            Future<Boolean> result =
                    executor.submit(VirtualThreadSupportTest::isCurrentThreadVirtual);

            assertTrue(result.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void workerSessionsNeverSilentlyDowngradeVirtualThreads() {
        FerricStoreClient client = FerricStoreClient.fromExecutor(command -> List.of());
        QueueWorker worker =
                new Queue(client, "email", "queued")
                        .worker("worker-1")
                        .concurrency(2)
                        .virtualThreads();

        if (Runtime.version().feature() < 21) {
            UnsupportedOperationException error =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () -> worker.openSession(job -> "ok"));
            assertTrue(error.getMessage().contains("require Java 21 or newer"));
            return;
        }

        try (QueueWorkerSession session = worker.openSession(job -> "ok")) {
            assertEquals(new QueueWorkerResult(0, 0, 0, 0), session.runOnce());
        }
    }

    private static boolean isCurrentThreadVirtual() throws Exception {
        Method isVirtual = Thread.class.getMethod("isVirtual");
        return (boolean) isVirtual.invoke(Thread.currentThread());
    }
}
