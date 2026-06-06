package com.ferricstore;

import java.util.List;

public final class Queue {
    private final FerricStoreClient client;
    private final String type;
    private final String state;

    Queue(FerricStoreClient client, String type, String state) {
        this.client = client;
        this.type = type;
        this.state = state;
    }

    public Object enqueue(String id, Object payload) {
        return client.create(
                CreateOptions.builder(id, type)
                        .state(state)
                        .payload(payload)
                        .idempotent(true)
                        .build());
    }

    public Object enqueueMany(List<CreateItem> items) {
        return client.createMany(
                CreateManyOptions.builder(type, items).state(state).independent(true).build());
    }

    public QueueWorker worker(String worker) {
        return new QueueWorker(client, type, state, worker);
    }
}
