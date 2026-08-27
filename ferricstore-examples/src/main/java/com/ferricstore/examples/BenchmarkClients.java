package com.ferricstore.examples;

import com.ferricstore.FerricStoreClient;
import com.ferricstore.HttpTransportOptions;
import com.ferricstore.RawCodec;
import java.net.http.HttpClient;
import java.util.Locale;

/** Shared transport setup for real-server example benchmarks. */
final class BenchmarkClients {
    private BenchmarkClients() {}

    static FerricStoreClient connect(
            String url, String httpVersion, int maxConcurrentRequests, int maxBatchItems) {
        return connect(url, httpVersion, maxConcurrentRequests, maxBatchItems, false);
    }

    static FerricStoreClient connect(
            String url,
            String httpVersion,
            int maxConcurrentRequests,
            int maxBatchItems,
            boolean compact) {
        if (!http(url)) {
            return FerricStoreClient.connect(url, new RawCodec());
        }
        HttpTransportOptions.Builder options =
                HttpTransportOptions.builder()
                        .preferredVersion(
                                "2".equals(httpVersion)
                                        ? HttpClient.Version.HTTP_2
                                        : HttpClient.Version.HTTP_1_1)
                        .maxConcurrentRequests(maxConcurrentRequests)
                        .maxBatchItems(Math.max(1_000, maxBatchItems))
                        .maxRequestBytes(64 * 1024 * 1024)
                        .maxResponseBytes(64 * 1024 * 1024)
                        .compact(compact);
        String bearerToken = System.getenv("FERRICSTORE_BEARER_TOKEN");
        String username = System.getenv("FERRICSTORE_USERNAME");
        String password = System.getenv("FERRICSTORE_PASSWORD");
        if (bearerToken != null && !bearerToken.isBlank()) {
            options.bearerToken(bearerToken);
        } else if (username != null && !username.isBlank()) {
            options.username(username).password(password == null ? "" : password);
            if (url.toLowerCase(Locale.ROOT).startsWith("http://")) {
                options.allowInsecureBasicAuthentication(true);
            }
        }
        return FerricStoreClient.connect(url, new RawCodec(), options.build());
    }

    static boolean http(String url) {
        String normalized = url.toLowerCase(Locale.ROOT);
        return normalized.startsWith("http://") || normalized.startsWith("https://");
    }
}
