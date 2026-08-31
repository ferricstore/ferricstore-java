package com.ferricstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** A blind TCP proxy that drops the first response after a test explicitly arms it. */
final class ResponseDropProxy implements AutoCloseable {
    private final URI upstream;
    private final ServerSocket listener;
    private final ExecutorService io;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean armed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch dropped = new CountDownLatch(1);

    private ResponseDropProxy(URI upstream) throws IOException {
        this.upstream = upstream;
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        AtomicInteger sequence = new AtomicInteger();
        io =
                Executors.newCachedThreadPool(
                        task -> {
                            Thread thread =
                                    new Thread(
                                            task,
                                            "response-drop-proxy-" + sequence.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });
        io.execute(this::acceptLoop);
    }

    static ResponseDropProxy open(String upstreamUrl) {
        try {
            URI upstream = URI.create(upstreamUrl);
            if (upstream.getHost() == null) {
                throw new IllegalArgumentException("upstream URL must include a host");
            }
            return new ResponseDropProxy(upstream);
        } catch (IOException failure) {
            throw new IllegalStateException("failed to open response-drop proxy", failure);
        }
    }

    String url() {
        return upstream.getScheme() + "://127.0.0.1:" + listener.getLocalPort();
    }

    void arm() {
        armed.set(true);
    }

    void awaitDropped(Duration timeout) {
        try {
            if (!dropped.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("proxy did not observe and drop the committed response");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the dropped response", failure);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeQuietly(listener);
        sockets.forEach(ResponseDropProxy::closeQuietly);
        io.shutdownNow();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket client = listener.accept();
                client.setTcpNoDelay(true);
                sockets.add(client);
                io.execute(() -> bridge(client));
            } catch (IOException failure) {
                if (!closed.get()) {
                    close();
                }
            }
        }
    }

    private void bridge(Socket client) {
        int port = upstream.getPort() >= 0 ? upstream.getPort() : defaultPort(upstream.getScheme());
        try {
            Socket server = new Socket(upstream.getHost(), port);
            server.setTcpNoDelay(true);
            sockets.add(server);
            AtomicBoolean armedRequestObserved = new AtomicBoolean();
            io.execute(() -> copyClientRequest(client, server, armedRequestObserved));
            copyServerResponse(server, client, armedRequestObserved);
        } catch (IOException failure) {
            closeQuietly(client);
        }
    }

    private void copyClientRequest(
            Socket source, Socket destination, AtomicBoolean armedRequestObserved) {
        try {
            copy(
                    source.getInputStream(),
                    destination.getOutputStream(),
                    () -> {
                        if (armed.get()) {
                            armedRequestObserved.set(true);
                        }
                        return false;
                    });
        } catch (IOException ignored) {
            closeQuietly(source);
            closeQuietly(destination);
        }
    }

    private void copyServerResponse(
            Socket source, Socket destination, AtomicBoolean armedRequestObserved) {
        try {
            copy(
                    source.getInputStream(),
                    destination.getOutputStream(),
                    () -> {
                        if (!armedRequestObserved.get()) {
                            return false;
                        }
                        try {
                            Thread.sleep(25);
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                        dropped.countDown();
                        close();
                        return true;
                    });
        } catch (IOException ignored) {
            closeQuietly(source);
            closeQuietly(destination);
        }
    }

    private static void copy(InputStream input, OutputStream output, DropDecision decision)
            throws IOException {
        byte[] buffer = new byte[16 * 1024];
        int count;
        while ((count = input.read(buffer)) >= 0) {
            if (decision.drop()) {
                return;
            }
            output.write(buffer, 0, count);
            output.flush();
        }
    }

    private static int defaultPort(String scheme) {
        return switch (scheme) {
            case "https", "ferrics" -> 443;
            case "http" -> 80;
            case "ferric" -> 6388;
            default -> throw new IllegalArgumentException("unsupported proxy scheme " + scheme);
        };
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best effort during an intentional transport failure.
        }
    }

    @FunctionalInterface
    private interface DropDecision {
        boolean drop();
    }
}
