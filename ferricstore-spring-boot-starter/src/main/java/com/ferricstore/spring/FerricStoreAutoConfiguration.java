package com.ferricstore.spring;

import com.ferricstore.Codec;
import com.ferricstore.FerricStoreClient;
import com.ferricstore.HttpTransportOptions;
import com.ferricstore.JsonCodec;
import com.ferricstore.NativeTransportOptions;
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
    FerricStoreClient ferricStoreClient(
            FerricStoreProperties properties,
            Codec codec,
            HttpTransportOptions httpOptions,
            NativeTransportOptions nativeOptions) {
        String url = properties.getUrl();
        String lower = url.toLowerCase(java.util.Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://")
                ? FerricStoreClient.connect(url, codec, httpOptions)
                : FerricStoreClient.connect(url, codec, nativeOptions);
    }

    @Bean
    @ConditionalOnMissingBean
    HttpTransportOptions ferricStoreHttpTransportOptions(FerricStoreProperties properties) {
        FerricStoreProperties.Http http = properties.getHttp();
        HttpTransportOptions.Builder builder =
                HttpTransportOptions.builder()
                        .headers(http.getHeaders())
                        .connectTimeout(http.getConnectTimeout())
                        .requestTimeout(http.getRequestTimeout())
                        .maxRequestBytes(http.getMaxRequestBytes())
                        .maxResponseBytes(http.getMaxResponseBytes())
                        .maxBatchItems(http.getMaxBatchItems())
                        .maxConcurrentRequests(http.getMaxConcurrentRequests())
                        .redirects(http.getRedirects())
                        .allowInsecureBasicAuthentication(
                                http.isAllowInsecureBasicAuthentication());
        if (hasText(http.getBearerToken())) {
            builder.bearerToken(http.getBearerToken());
        }
        if (hasText(http.getUsername())) {
            builder.username(http.getUsername()).password(http.getPassword());
        }
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    NativeTransportOptions ferricStoreNativeTransportOptions() {
        return NativeTransportOptions.defaults();
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

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
