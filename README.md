# gcp-spring-boot-starter
Spring Boot starter for Google Cloud Platform services

## Google Cloud Storage

Library adds `com.google.cloud.storage.Storage` bean auto configuration and support for `gcs://` resources loading via
`org.springframework.core.io.support.ResourcePatternResolver`

## Google Cloud Spanner

Library adds `com.google.cloud.spanner.DatabaseClient` bean auto configuration and provides `SpannerHelper` utility 
class with useful static methods.
