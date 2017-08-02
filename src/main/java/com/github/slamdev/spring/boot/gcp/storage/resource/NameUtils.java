package com.github.slamdev.spring.boot.gcp.storage.resource;

import org.springframework.util.Assert;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

final class NameUtils {

    private static final String PROTOCOL_PREFIX = "gcs://";
    private static final String PATH_DELIMITER = "/";
    private static final String VERSION_DELIMITER = "^";

    private NameUtils() {
        // Utility class
    }

    static boolean isStorageResource(String location) {
        requireNonNull(location, "Location must not be null");
        return location.toLowerCase(ENGLISH).startsWith(PROTOCOL_PREFIX);
    }

    static String getBucketNameFromLocation(String location) {
        requireNonNull(location, "Location must not be null");
        if (!isStorageResource(location)) {
            throw new IllegalArgumentException("The location :'" + location + "' is not a valid GCE location");
        }
        int bucketEndIndex = location.indexOf(PATH_DELIMITER, PROTOCOL_PREFIX.length());
        if (bucketEndIndex == -1 || bucketEndIndex == PROTOCOL_PREFIX.length()) {
            throw new IllegalArgumentException("The location :'" + location + "' does not contain a valid bucket name");
        }
        return location.substring(PROTOCOL_PREFIX.length(), bucketEndIndex);
    }

    static String getObjectNameFromLocation(String location) {
        requireNonNull(location, "Location must not be null");
        if (!isStorageResource(location)) {
            throw new IllegalArgumentException("The location :'" + location + "' is not a valid GCE location");
        }
        int bucketEndIndex = location.indexOf(PATH_DELIMITER, PROTOCOL_PREFIX.length());
        if (bucketEndIndex == -1 || bucketEndIndex == PROTOCOL_PREFIX.length()) {
            throw new IllegalArgumentException("The location :'" + location + "' does not contain a valid bucket name");
        }
        if (location.contains(VERSION_DELIMITER)) {
            return getObjectNameFromLocation(location.substring(0, location.indexOf(VERSION_DELIMITER)));
        }
        if (location.endsWith(PATH_DELIMITER)) {
            return location.substring(++bucketEndIndex, location.length() - 1);
        }
        return location.substring(++bucketEndIndex, location.length());
    }

    static String stripProtocol(String location) {
        requireNonNull(location, "Location must not be null");
        if (!isStorageResource(location)) {
            throw new IllegalArgumentException("The location :'" + location + "' is not a valid gcs location");
        }
        return location.substring(PROTOCOL_PREFIX.length());
    }

    @SuppressWarnings("StringBufferReplaceableByString")
    static String getLocationForBucketAndObject(String bucketName, String objectName) {
        Assert.notNull(bucketName, "Bucket name must not be null");
        Assert.notNull(objectName, "ObjectName name must not be null");
        StringBuilder location = new StringBuilder(PROTOCOL_PREFIX.length()
                + bucketName.length()
                + PATH_DELIMITER.length()
                + objectName.length());
        location.append(PROTOCOL_PREFIX);
        location.append(bucketName);
        location.append(PATH_DELIMITER);
        location.append(objectName);
        return location.toString();
    }
}
