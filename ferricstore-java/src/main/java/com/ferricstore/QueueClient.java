package com.ferricstore;

public final class QueueClient {
    private final FerricStoreClient client;

    public QueueClient(FerricStoreClient client) {
        this.client = client;
    }

    public Queue queue(String type) {
        return new Queue(client, type, "queued");
    }

    public Queue queue(String type, String state) {
        return new Queue(client, type, state);
    }
}
