package com.ferricstore.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.ferricstore.Codec;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.QueueClient;
import com.ferricstore.RedisExecutor;
import com.ferricstore.WorkflowClient;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

final class FerricStoreAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(FerricStoreAutoConfiguration.class))
        .withUserConfiguration(TestClientConfiguration.class);

    @Test
    void createsQueueAndWorkflowClientsFromExistingFerricStoreClient() {
        runner.run(context -> assertThat(context)
            .hasSingleBean(FerricStoreClient.class)
            .hasSingleBean(QueueClient.class)
            .hasSingleBean(WorkflowClient.class)
            .hasSingleBean(Codec.class));
    }

    @Test
    void supportsJsonCodecProperty() {
        runner.withPropertyValues("ferricstore.codec=json")
            .run(context -> assertThat(context.getBean(Codec.class)).isInstanceOf(JsonCodec.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class TestClientConfiguration {
        @Bean
        FerricStoreClient ferricStoreClient(Codec codec) {
            RedisExecutor executor = args -> "OK";
            return FerricStoreClient.fromExecutor(executor, codec);
        }
    }
}
