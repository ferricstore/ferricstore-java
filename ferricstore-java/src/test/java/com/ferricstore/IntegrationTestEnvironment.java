package com.ferricstore;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

final class IntegrationTestEnvironment {
    private IntegrationTestEnvironment() {}

    static FerricStoreClient connectJson() {
        return connect(new JsonCodec());
    }

    static FerricStoreClient connectJsonAt(String url) {
        return connect(new JsonCodec(), url);
    }

    static FerricStoreClient connectRaw() {
        return connect(new RawCodec());
    }

    static String suffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    static void assumeIntegration() {
        assumeTrue(
                "1".equals(System.getenv("FERRICSTORE_INTEGRATION")),
                "set FERRICSTORE_INTEGRATION=1 to run local FerricStore integration tests");
    }

    static boolean isHttpIntegration() {
        String url = transportUrl();
        return url.startsWith("http://") || url.startsWith("https://");
    }

    static String transportUrl() {
        return System.getenv().getOrDefault("FERRICSTORE_URL", "ferric://127.0.0.1:6388");
    }

    private static FerricStoreClient connect(Codec codec) {
        return connect(codec, transportUrl());
    }

    private static FerricStoreClient connect(Codec codec, String url) {
        String caFile = System.getenv("FERRICSTORE_CA_FILE");
        if (!(url.startsWith("http://") || url.startsWith("https://"))) {
            NativeTransportOptions.Builder options = NativeTransportOptions.builder();
            if (caFile != null && !caFile.isBlank()) {
                options.sslContext(trustContext(Path.of(caFile)));
            }
            return FerricStoreClient.connect(url, codec, options.build());
        }
        HttpTransportOptions.Builder options =
                HttpTransportOptions.builder()
                        .username(requiredEnvironment("FERRICSTORE_USERNAME"))
                        .password(requiredEnvironment("FERRICSTORE_PASSWORD"))
                        .maxConcurrentRequests(32)
                        .compact(
                                "msgpack"
                                        .equals(
                                                System.getenv()
                                                        .getOrDefault(
                                                                "FERRICSTORE_HTTP_FORMAT",
                                                                "json")));
        String version = System.getenv().getOrDefault("FERRICSTORE_HTTP_VERSION", "h1");
        options.preferredVersion(
                switch (version) {
                    case "h1" -> HttpClient.Version.HTTP_1_1;
                    case "h2" -> HttpClient.Version.HTTP_2;
                    default ->
                            throw new IllegalStateException(
                                    "FERRICSTORE_HTTP_VERSION must be h1 or h2");
                });
        if (url.startsWith("http://")) {
            options.allowInsecureBasicAuthentication(true);
        }
        if (caFile != null && !caFile.isBlank()) {
            options.sslContext(trustContext(Path.of(caFile)));
        }
        return FerricStoreClient.connect(url, codec, options.build());
    }

    static void assertExpectedHttpVersion(FerricStoreClient client) {
        if (!isHttpIntegration()) {
            return;
        }
        HttpClient.Version expected =
                "h2".equals(System.getenv().getOrDefault("FERRICSTORE_HTTP_VERSION", "h1"))
                        ? HttpClient.Version.HTTP_2
                        : HttpClient.Version.HTTP_1_1;
        if (client.observedHttpVersion() != expected) {
            throw new AssertionError(
                    "expected " + expected + " but observed " + client.observedHttpVersion());
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for HTTP integration tests");
        }
        return value;
    }

    private static SSLContext trustContext(Path certificateFile) {
        try (java.io.InputStream input = Files.newInputStream(certificateFile)) {
            Certificate certificate =
                    CertificateFactory.getInstance("X.509").generateCertificate(input);
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ferricstore-http", certificate);
            TrustManagerFactory trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagers.getTrustManagers(), null);
            return context;
        } catch (java.io.IOException | java.security.GeneralSecurityException error) {
            throw new IllegalStateException("failed to load FerricStore integration CA", error);
        }
    }
}
