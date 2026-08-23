package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class HttpIntegrationReleaseContractTest {
    private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();

    @Test
    void ciReleaseAndDocumentationRequireAuthenticatedTlsHttpIntegration() throws IOException {
        String runner = repositoryFile("scripts/run-http-integration.sh");
        assertImmutableFerricStoreImages(runner);
        for (String required :
                new String[] {
                    "FERRICSTORE_HTTP_TLS_ENABLED=true",
                    "FERRICSTORE_HTTP_AUTH_CACHE_ENABLED=true",
                    "FERRICSTORE_AUTH_RATE_LIMIT_MAX_ATTEMPTS=100000",
                    "FERRICSTORE_USERNAME",
                    "FERRICSTORE_PASSWORD",
                    "FERRICSTORE_CA_FILE",
                    "@sha256:",
                    "chmod 700",
                    "chmod 600",
                    "rm -f \"$tls_dir/ca.key\"",
                    "source=$tls_dir/server.key,target=/tls/server.key,readonly",
                    "sdk-http-denied",
                    "ACL authorization probe unexpectedly allowed SET",
                    "unauthenticated HTTP request returned",
                    "FerricStoreIntegrationTest",
                    "FerricStoreConcurrencyIntegrationTest"
                }) {
            assertTrue(runner.contains(required), () -> "runner is missing " + required);
        }

        for (String workflow :
                new String[] {".github/workflows/test.yml", ".github/workflows/release.yml"}) {
            String contents = repositoryFile(workflow);
            assertTrue(contents.contains("scripts/run-http-integration.sh"));
            assertTrue(contents.contains("@sha256:"));
            assertImmutableFerricStoreImages(contents);
        }

        String readme = repositoryFile("README.md");
        assertTrue(readme.contains("run-http-integration.sh"));
        assertTrue(readme.contains("FERRICSTORE_CA_FILE"));
    }

    @Test
    void releaseValidationKeepsRepositoryWritePermissionAtThePublishBoundary() throws IOException {
        String release = repositoryFile(".github/workflows/release.yml");
        assertTrue(release.contains("permissions:\n  contents: read"));
        assertTrue(
                release.contains(
                        "maven-central:\n"
                                + "    name: publish maven central\n"
                                + "    needs: validate\n"
                                + "    permissions:\n"
                                + "      contents: write"));
    }

    private static void assertImmutableFerricStoreImages(String contents) {
        contents.lines()
                .filter(line -> line.contains("quay.io/ferricstore/ferricstore:"))
                .forEach(line -> assertTrue(line.contains("@sha256:"), line));
    }

    private static String repositoryFile(String path) throws IOException {
        return Files.readString(REPOSITORY.resolve(path));
    }
}
