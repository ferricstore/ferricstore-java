package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class HttpIntegrationReleaseContractTest {
    private static final Path REPOSITORY = Path.of("..").toAbsolutePath().normalize();
    private static final String RELEASE_IMAGE =
            "quay.io/ferricstore/ferricstore:0.11.12@sha256:"
                    + "3aef2c4200dff987a5797c08548582136d827838ccc0286aef5a00e8f4f6aa62";
    private static final String INTEGRATION_TESTS =
            "-Dtest=FerricStoreIntegrationTest,FerricStoreCommandArgumentsIntegrationTest,"
                    + "FerricStoreFlowArgumentsIntegrationTest,"
                    + "FerricStoreConcurrencyIntegrationTest";

    @Test
    void ciReleaseAndDocumentationRequireAuthenticatedTlsHttpIntegration() throws IOException {
        String runner = repositoryFile("scripts/run-http-integration.sh");
        assertImmutableFerricStoreImages(runner);
        assertTrue(runner.contains(RELEASE_IMAGE));
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
                    "FerricStoreFlowArgumentsIntegrationTest",
                    "FerricStoreConcurrencyIntegrationTest"
                }) {
            assertTrue(runner.contains(required), () -> "runner is missing " + required);
        }

        for (String workflow :
                new String[] {".github/workflows/test.yml", ".github/workflows/release.yml"}) {
            String contents = repositoryFile(workflow);
            assertTrue(contents.contains("scripts/run-http-integration.sh"));
            assertTrue(contents.contains("@sha256:"));
            assertTrue(contents.contains(RELEASE_IMAGE));
            assertImmutableFerricStoreImages(contents);
        }

        String compose = repositoryFile("docker-compose.yml");
        assertTrue(compose.contains(RELEASE_IMAGE));
        assertImmutableFerricStoreImages(compose);

        String readme = repositoryFile("README.md");
        assertTrue(readme.contains("run-http-integration.sh"));
        assertTrue(readme.contains("FERRICSTORE_CA_FILE"));
        assertTrue(readme.contains(RELEASE_IMAGE));
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

    @Test
    void liveTcpAndHttpIntegrationsRunOnJava17AndJava21() throws IOException {
        String workflow = repositoryFile(".github/workflows/test.yml");
        String tcpIntegration = job(workflow, "  integration:\n", "  http-integration:\n");
        String httpIntegration = job(workflow, "  http-integration:\n", null);

        for (String integration : new String[] {tcpIntegration, httpIntegration}) {
            assertTrue(integration.contains("java-version: [17, 21]"));
            assertTrue(integration.contains("java-version: ${{ matrix.java-version }}"));
            assertTrue(integration.contains("integration java-${{ matrix.java-version }}"));
        }

        String mise = repositoryFile("mise.toml");
        for (String task :
                new String[] {
                    "[tasks.\"integration:java17\"]",
                    "[tasks.\"integration:java21\"]",
                    "[tasks.\"integration:http:java17\"]",
                    "[tasks.\"integration:http:java21\"]"
                }) {
            assertTrue(mise.contains(task), () -> "mise is missing " + task);
        }
        assertTrue(
                mise.contains(
                        "export FERRICSTORE_WAIT_SECONDS=\"${FERRICSTORE_WAIT_SECONDS:-120}\""),
                "candidate integration must tolerate a cold multi-shard OSS startup");

        String releaseWorkflow = repositoryFile(".github/workflows/release.yml");
        String releaseValidation = job(releaseWorkflow, "  validate:\n", "  maven-central:\n");
        assertTrue(releaseValidation.contains("java-version: [17, 21]"));
        assertTrue(releaseValidation.contains("java-version: ${{ matrix.java-version }}"));
        assertTrue(
                releaseValidation.contains(
                        "if: matrix.java-version == 17\n        run: mvn -B test"));
        assertTrue(
                releaseValidation.contains(
                        "if: matrix.java-version == 21\n        run: mvn -B -P quality verify"));
        assertTrue(releaseValidation.contains(INTEGRATION_TESTS));
        assertTrue(releaseValidation.contains("scripts/run-http-integration.sh"));
    }

    private static void assertImmutableFerricStoreImages(String contents) {
        contents.lines()
                .filter(line -> line.contains("quay.io/ferricstore/ferricstore:"))
                .forEach(line -> assertTrue(line.contains("@sha256:"), line));
    }

    private static String job(String workflow, String startMarker, String endMarker) {
        int start = workflow.indexOf(startMarker);
        assertTrue(start >= 0, () -> "workflow is missing " + startMarker.trim());
        int end = endMarker == null ? workflow.length() : workflow.indexOf(endMarker, start + 1);
        assertTrue(end >= 0, () -> "workflow is missing " + endMarker.trim());
        return workflow.substring(start, end);
    }

    private static String repositoryFile(String path) throws IOException {
        return Files.readString(REPOSITORY.resolve(path));
    }
}
