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
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;

@EnableConfigurationProperties(GcpProperties.class)
@Configuration
public class GcpAutoConfiguration {

    @ConditionalOnClass(GoogleCredentials.class)
    @Configuration
    public static class ForGoogleCredentials {

        @ConditionalOnMissingBean
        @Bean
        public GoogleCredentials googleCredentials(Environment environment, ResourceLoader resourceLoader) throws IOException {
            String path = environment.getProperty("google.cloud.credentials-resource");
            return GoogleCredentials.fromStream(resourceLoader.getResource(path).getInputStream());
        }
    }

    @ConditionalOnClass(value = Storage.class)
    @Configuration
    public static class ForStorage {

        @ConditionalOnMissingBean
        @Bean
        public Storage storage(GoogleCredentials googleCredentials, Environment environment) {
            return StorageOptions.newBuilder()
                    .setCredentials(googleCredentials)
                    .setProjectId(environment.getProperty("google.cloud.project-id"))
                    .build()
                    .getService();
        }


    }

    @ConditionalOnClass(value = Storage.class)
    @Configuration
    public static class ForStorageResource {

        @ConditionalOnMissingBean
        @Bean
        public static StorageResourceLoaderBeanPostProcessor storageResourceLoaderBeanPostProcessor(Storage storage) {
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
        public DatabaseClient databaseClient() {
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
            DatabaseId databaseId = DatabaseId.of(properties.getProjectId(), properties.getSpanner().getInstance(),
                    properties.getSpanner().getDatabase());
            return spanner.getDatabaseClient(databaseId);
        }
    }
}
