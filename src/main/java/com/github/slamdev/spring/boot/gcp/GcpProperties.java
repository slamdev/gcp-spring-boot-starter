package com.github.slamdev.spring.boot.gcp;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;

/**
 * The configuration properties for gcp.
 */
@Data
@ConfigurationProperties("google.cloud")
public class GcpProperties {

    /**
     * The location of the Service Account key file in JSON format from the Google Developers Console
     * or a stored user credential using the format supported by the Cloud SDK.
     */
    private Resource credentialsResource;

    /**
     * Google Cloud project id
     */
    private String projectId;

    private Spanner spanner;

    private Storage storage;

    @Data
    public static class Spanner {

        /**
         * Spanner instance id
         */
        private String instance;

        /**
         * Spanner database name
         */
        private String database;
    }

    @Data
    public static class Storage {
    }
}
