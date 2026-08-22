package com.ferricstore;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;

/** Configuration for the Java 17 HTTP/HTTPS command transport. */
public final class HttpTransportOptions {
    static final int DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;
    static final int DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    static final int DEFAULT_MAX_BATCH_ITEMS = 1_000;
    static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 100;

    private final String bearerToken;
    private final String username;
    private final String password;
    private final Map<String, String> headers;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final int maxRequestBytes;
    private final int maxResponseBytes;
    private final int maxBatchItems;
    private final int maxConcurrentRequests;
    private final HttpClient.Version preferredVersion;
    private final HttpClient.Redirect redirects;
    private final SSLContext sslContext;
    private final HttpClient httpClient;
    private final boolean allowInsecureBasicAuthentication;

    private HttpTransportOptions(Builder builder) {
        bearerToken = builder.bearerToken;
        username = builder.username;
        password = builder.password;
        headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        connectTimeout = builder.connectTimeout;
        requestTimeout = builder.requestTimeout;
        maxRequestBytes = builder.maxRequestBytes;
        maxResponseBytes = builder.maxResponseBytes;
        maxBatchItems = builder.maxBatchItems;
        maxConcurrentRequests = builder.maxConcurrentRequests;
        preferredVersion = builder.preferredVersion;
        redirects = builder.redirects;
        sslContext = builder.sslContext;
        httpClient = builder.httpClient;
        allowInsecureBasicAuthentication = builder.allowInsecureBasicAuthentication;
    }

    public static HttpTransportOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    String bearerToken() {
        return bearerToken;
    }

    String username() {
        return username;
    }

    String password() {
        return password;
    }

    Map<String, String> headers() {
        return headers;
    }

    Duration connectTimeout() {
        return connectTimeout;
    }

    Duration requestTimeout() {
        return requestTimeout;
    }

    int maxRequestBytes() {
        return maxRequestBytes;
    }

    int maxResponseBytes() {
        return maxResponseBytes;
    }

    int maxBatchItems() {
        return maxBatchItems;
    }

    int maxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    HttpClient.Version preferredVersion() {
        return preferredVersion;
    }

    HttpClient.Redirect redirects() {
        return redirects;
    }

    SSLContext sslContext() {
        return sslContext;
    }

    HttpClient httpClient() {
        return httpClient;
    }

    boolean allowInsecureBasicAuthentication() {
        return allowInsecureBasicAuthentication;
    }

    public static final class Builder {
        private String bearerToken;
        private String username;
        private String password;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private int maxRequestBytes = DEFAULT_MAX_REQUEST_BYTES;
        private int maxResponseBytes = DEFAULT_MAX_RESPONSE_BYTES;
        private int maxBatchItems = DEFAULT_MAX_BATCH_ITEMS;
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private HttpClient.Version preferredVersion = HttpClient.Version.HTTP_1_1;
        private HttpClient.Redirect redirects = HttpClient.Redirect.ALWAYS;
        private SSLContext sslContext;
        private HttpClient httpClient;
        private boolean allowInsecureBasicAuthentication;

        private Builder() {}

        public Builder bearerToken(String value) {
            bearerToken = nonBlank(value, "bearerToken");
            return this;
        }

        public Builder username(String value) {
            username = nonBlank(value, "username");
            return this;
        }

        public Builder password(String value) {
            if (value == null) {
                throw new IllegalArgumentException("password cannot be null");
            }
            rejectNewlines(value, "password");
            password = value;
            return this;
        }

        public Builder header(String name, String value) {
            String checkedName = nonBlank(name, "header name");
            String checkedValue = nonNull(value, "header value");
            rejectNewlines(checkedName, "header name");
            rejectNewlines(checkedValue, "header value");
            headers.put(checkedName, checkedValue);
            return this;
        }

        public Builder headers(Map<String, String> values) {
            if (values == null) {
                throw new IllegalArgumentException("headers cannot be null");
            }
            values.forEach(this::header);
            return this;
        }

        public Builder connectTimeout(Duration value) {
            connectTimeout = positive(value, "connectTimeout");
            return this;
        }

        public Builder requestTimeout(Duration value) {
            requestTimeout = positive(value, "requestTimeout");
            return this;
        }

        public Builder maxRequestBytes(int value) {
            maxRequestBytes = positive(value, "maxRequestBytes");
            return this;
        }

        public Builder maxResponseBytes(int value) {
            maxResponseBytes = positive(value, "maxResponseBytes");
            return this;
        }

        public Builder maxBatchItems(int value) {
            maxBatchItems = positive(value, "maxBatchItems");
            return this;
        }

        public Builder maxConcurrentRequests(int value) {
            maxConcurrentRequests = positive(value, "maxConcurrentRequests");
            return this;
        }

        public Builder preferredVersion(HttpClient.Version value) {
            if (value == null) {
                throw new IllegalArgumentException("preferredVersion cannot be null");
            }
            preferredVersion = value;
            return this;
        }

        public Builder redirects(HttpClient.Redirect value) {
            if (value == null) {
                throw new IllegalArgumentException("redirects cannot be null");
            }
            redirects = value;
            return this;
        }

        public Builder sslContext(SSLContext value) {
            sslContext = value;
            return this;
        }

        /** Uses a caller-managed client. Its proxy, TLS, redirect, and connection settings win. */
        public Builder httpClient(HttpClient value) {
            if (value == null) {
                throw new IllegalArgumentException("httpClient cannot be null");
            }
            httpClient = value;
            return this;
        }

        /** Allows Basic credentials over {@code http://}; use only behind a trusted TLS ingress. */
        public Builder allowInsecureBasicAuthentication(boolean value) {
            allowInsecureBasicAuthentication = value;
            return this;
        }

        public HttpTransportOptions build() {
            boolean authorizationHeader =
                    headers.keySet().stream().anyMatch("authorization"::equalsIgnoreCase);
            boolean basic = username != null || password != null;
            if (username != null && password == null) {
                throw new IllegalArgumentException("username requires password");
            }
            if (username != null && username.contains(":")) {
                throw new IllegalArgumentException("username cannot contain ':'");
            }
            if (bearerToken != null && basic) {
                throw new IllegalArgumentException(
                        "bearerToken and username/password are mutually exclusive");
            }
            if (authorizationHeader && (bearerToken != null || basic)) {
                throw new IllegalArgumentException(
                        "explicit authentication and an Authorization header are mutually exclusive");
            }
            return new HttpTransportOptions(this);
        }

        private static Duration positive(Duration value, String name) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }

        private static int positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }

        private static String nonBlank(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
            rejectNewlines(value, name);
            return value;
        }

        private static String nonNull(String value, String name) {
            if (value == null) {
                throw new IllegalArgumentException(name + " cannot be null");
            }
            return value;
        }

        private static void rejectNewlines(String value, String name) {
            if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
                throw new IllegalArgumentException(name + " cannot contain newlines");
            }
        }
    }
}
