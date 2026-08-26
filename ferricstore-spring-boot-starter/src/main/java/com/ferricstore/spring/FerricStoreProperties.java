package com.ferricstore.spring;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ferricstore")
public class FerricStoreProperties {
    /** Native FerricStore URL used by the SDK client. */
    private String url = "ferric://127.0.0.1:6388";

    /** Codec used for payloads, results, and named values. */
    private CodecMode codec = CodecMode.RAW;

    /** HTTP/HTTPS transport settings; ignored for native TCP/TLS URLs. */
    private final Http http = new Http();

    /** Native TCP/TLS transport settings; ignored for HTTP/HTTPS URLs. */
    private final Native nativeTransport = new Native();

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public CodecMode getCodec() {
        return codec;
    }

    public void setCodec(CodecMode codec) {
        this.codec = codec;
    }

    public Http getHttp() {
        return http;
    }

    public Native getNative() {
        return nativeTransport;
    }

    public static class Http {
        private String bearerToken;
        private String username;
        private String password;
        private Map<String, String> headers = new LinkedHashMap<>();
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private int maxRequestBytes = 1_048_576;
        private int maxResponseBytes = 16_777_216;
        private int maxBatchItems = 1_000;
        private int maxConcurrentRequests = 100;
        private int maxPendingRequests = 1_000;
        private HttpClient.Redirect redirects = HttpClient.Redirect.ALWAYS;
        private boolean allowInsecureBasicAuthentication;
        private boolean compact;

        public String getBearerToken() {
            return bearerToken;
        }

        public void setBearerToken(String bearerToken) {
            this.bearerToken = bearerToken;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, String> headers) {
            this.headers = new LinkedHashMap<>(headers);
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public int getMaxRequestBytes() {
            return maxRequestBytes;
        }

        public void setMaxRequestBytes(int maxRequestBytes) {
            this.maxRequestBytes = maxRequestBytes;
        }

        public int getMaxResponseBytes() {
            return maxResponseBytes;
        }

        public void setMaxResponseBytes(int maxResponseBytes) {
            this.maxResponseBytes = maxResponseBytes;
        }

        public int getMaxBatchItems() {
            return maxBatchItems;
        }

        public void setMaxBatchItems(int maxBatchItems) {
            this.maxBatchItems = maxBatchItems;
        }

        public int getMaxConcurrentRequests() {
            return maxConcurrentRequests;
        }

        public void setMaxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
        }

        public int getMaxPendingRequests() {
            return maxPendingRequests;
        }

        public void setMaxPendingRequests(int maxPendingRequests) {
            this.maxPendingRequests = maxPendingRequests;
        }

        public HttpClient.Redirect getRedirects() {
            return redirects;
        }

        public void setRedirects(HttpClient.Redirect redirects) {
            this.redirects = redirects;
        }

        public boolean isAllowInsecureBasicAuthentication() {
            return allowInsecureBasicAuthentication;
        }

        public void setAllowInsecureBasicAuthentication(boolean value) {
            allowInsecureBasicAuthentication = value;
        }

        public boolean isCompact() {
            return compact;
        }

        public void setCompact(boolean value) {
            compact = value;
        }
    }

    public static class Native {
        private int maxPendingRequests = 1_024;

        public int getMaxPendingRequests() {
            return maxPendingRequests;
        }

        public void setMaxPendingRequests(int maxPendingRequests) {
            this.maxPendingRequests = maxPendingRequests;
        }
    }

    public enum CodecMode {
        RAW,
        JSON,
        STRING
    }
}
