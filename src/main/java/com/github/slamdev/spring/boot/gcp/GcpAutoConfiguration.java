package com.github.slamdev.spring.boot.gcp;

import com.github.slamdev.spring.boot.gcp.storage.resource.StorageResourceLoaderBeanPostProcessor;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.*;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(GcpProperties.class)
@Configuration
public class GcpAutoConfiguration {

    @RequiredArgsConstructor
    @ConditionalOnClass(value = Storage.class)
    @Configuration
    public static class ForStorage {

        private final GcpProperties properties;

        private final GoogleCredentials googleCredentials;

        @ConditionalOnMissingBean
        @Bean
        public Storage storage() {
            return StorageOptions.newBuilder()
                    .setCredentials(googleCredentials)
                    .setProjectId(properties.getProjectId())
                    .build()
                    .getService();
        }

        @ConditionalOnMissingBean
        @Bean
        public StorageResourceLoaderBeanPostProcessor storageResourceLoaderBeanPostProcessor(Storage storage) {
            return new StorageResourceLoaderBeanPostProcessor(storage);
        }
    }

    @RequiredArgsConstructor
    @ConditionalOnClass(value = Storage.class)
    @Configuration
    public static class ForSpanner {

        private final GcpProperties properties;

        private final GoogleCredentials googleCredentials;

        @ConditionalOnMissingBean
        @Bean
        public DatabaseClient storage() {
            SpannerOptions options = SpannerOptions.newBuilder()
                    .setProjectId(properties.getProjectId())
                    .setNumChannels(50)
                    .setSessionPoolOption(SessionPoolOptions.newBuilder()
                            .setBlockIfPoolExhausted()
                            .setMaxSessions(5000)
                            .build())
                    .setCredentials(googleCredentials)
                    .build();
            Spanner spanner = options.getService();
            DatabaseId databaseId = DatabaseId.of(properties.getProjectId(), properties.getForSpanner().getInstance(),
                    properties.getForSpanner().getDatabase());
            return spanner.getDatabaseClient(databaseId);
        }
    }
}
