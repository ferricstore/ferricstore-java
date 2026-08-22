package com.ferricstore.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.ferricstore.Codec;
import com.ferricstore.CommandExecutor;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.QueueClient;
import com.ferricstore.WorkflowClient;
import com.ferricstore.spring.statemachine.FerricFlowStateMachine;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.StateMachineFactory;

final class FerricStoreAutoConfigurationTest {
    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    FerricStoreAutoConfiguration.class,
                                    FerricStoreStateMachineAutoConfiguration.class))
                    .withUserConfiguration(TestClientConfiguration.class);

    @Test
    void createsQueueAndWorkflowClientsFromExistingFerricStoreClient() {
        runner.run(
                context ->
                        assertThat(context)
                                .hasSingleBean(FerricStoreClient.class)
                                .hasSingleBean(QueueClient.class)
                                .hasSingleBean(WorkflowClient.class)
                                .hasSingleBean(Codec.class));
    }

    @Test
    void supportsJsonCodecProperty() {
        runner.withPropertyValues("ferricstore.codec=json")
                .run(
                        context ->
                                assertThat(context.getBean(Codec.class))
                                        .isInstanceOf(JsonCodec.class));
    }

    @Test
    void defaultsToTheFerricStoreNativeEndpoint() {
        assertThat(new FerricStoreProperties().getUrl()).isEqualTo("ferric://127.0.0.1:6388");
    }

    @Test
    void bindsHttpAuthenticationWithoutMakingSpringRequiredByTheCore() {
        runner.withPropertyValues(
                        "ferricstore.url=https://gateway.example",
                        "ferricstore.http.username=lambda",
                        "ferricstore.http.password=secret",
                        "ferricstore.http.max-concurrent-requests=32")
                .run(
                        context -> {
                            FerricStoreProperties properties =
                                    context.getBean(FerricStoreProperties.class);
                            assertThat(properties.getHttp().getUsername()).isEqualTo("lambda");
                            assertThat(properties.getHttp().getPassword()).isEqualTo("secret");
                            assertThat(properties.getHttp().getMaxConcurrentRequests())
                                    .isEqualTo(32);
                        });
    }

    @Test
    void createsFerricFlowStateMachineFromSingleSpringFactory() {
        runner.withUserConfiguration(TestStateMachineConfiguration.class)
                .run(context -> assertThat(context).hasSingleBean(FerricFlowStateMachine.class));
    }

    @Test
    void loadsStarterWithoutOptionalStateMachineAdapter() {
        runner.withClassLoader(
                        new FilteredClassLoader(
                                FerricFlowStateMachine.class, StateMachineFactory.class))
                .run(
                        context ->
                                assertThat(context)
                                        .hasSingleBean(FerricStoreClient.class)
                                        .doesNotHaveBean(FerricFlowStateMachine.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class TestClientConfiguration {
        @Bean
        FerricStoreClient ferricStoreClient(Codec codec) {
            CommandExecutor executor = args -> "OK";
            return FerricStoreClient.fromExecutor(executor, codec);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TestStateMachineConfiguration {
        @Bean
        StateMachineFactory<String, String> orderMachine() throws Exception {
            StateMachineBuilder.Builder<String, String> builder = StateMachineBuilder.builder();
            builder.configureStates()
                    .withStates()
                    .initial("created")
                    .state("charged")
                    .end("completed");
            builder.configureTransitions()
                    .withExternal()
                    .source("created")
                    .target("charged")
                    .event("CHARGE");
            return builder.createFactory();
        }
    }
}
