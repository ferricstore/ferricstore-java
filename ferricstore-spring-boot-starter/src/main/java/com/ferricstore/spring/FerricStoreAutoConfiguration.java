package com.ferricstore.spring;

import com.ferricstore.Codec;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.JsonCodec;
import com.ferricstore.QueueClient;
import com.ferricstore.RawCodec;
import com.ferricstore.StringCodec;
import com.ferricstore.WorkflowClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(FerricStoreProperties.class)
public class FerricStoreAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    Codec ferricStoreCodec(FerricStoreProperties properties) {
        return switch (properties.getCodec()) {
            case JSON -> new JsonCodec();
            case STRING -> new StringCodec();
            case RAW -> new RawCodec();
        };
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    FerricStoreClient ferricStoreClient(FerricStoreProperties properties, Codec codec) {
        return FerricStoreClient.connect(properties.getUrl(), codec);
    }

    @Bean
    @ConditionalOnMissingBean
    QueueClient ferricStoreQueueClient(FerricStoreClient client) {
        return new QueueClient(client);
    }

    @Bean
    @ConditionalOnMissingBean
    WorkflowClient ferricStoreWorkflowClient(FerricStoreClient client) {
        return new WorkflowClient(client);
    }
}
