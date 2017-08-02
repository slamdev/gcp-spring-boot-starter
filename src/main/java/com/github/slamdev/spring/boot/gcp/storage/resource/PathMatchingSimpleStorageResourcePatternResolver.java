package com.github.slamdev.spring.boot.gcp.storage.resource;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.PathMatcher;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class PathMatchingSimpleStorageResourcePatternResolver implements ResourcePatternResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(PathMatchingSimpleStorageResourcePatternResolver.class);
    private final Storage storage;
    private final ResourceLoader simpleStorageResourceLoader;
    private final ResourcePatternResolver resourcePatternResolverDelegate;
    private PathMatcher pathMatcher = new AntPathMatcher();

    /**
     * Construct a new instance of the {@link PathMatchingSimpleStorageResourcePatternResolver} with a
     * {@link Storage} to load gcs instances, and also a delegate {@link ResourcePatternResolver}
     * to resolve resource on default path (like file and classpath)
     */
    public PathMatchingSimpleStorageResourcePatternResolver(Storage storage, ResourceLoader simpleStorageResourceLoader,
                                                            ResourcePatternResolver resourcePatternResolverDelegate) {
        this.storage = storage;
        this.simpleStorageResourceLoader = simpleStorageResourceLoader;
        this.resourcePatternResolverDelegate = resourcePatternResolverDelegate;
    }

    /**
     * Set the PathMatcher implementation to use for this
     * resource pattern resolver. Default is AntPathMatcher.
     *
     * @param pathMatcher The pathMatches implementation used, must not be null
     * @see AntPathMatcher
     */
    public void setPathMatcher(PathMatcher pathMatcher) {
        Assert.notNull(pathMatcher, "PathMatcher must not be null");
        this.pathMatcher = pathMatcher;
    }

    @Override
    public Resource[] getResources(String locationPattern) throws IOException {
        if (NameUtils.isStorageResource(locationPattern)) {
            if (this.pathMatcher.isPattern(NameUtils.stripProtocol(locationPattern))) {
                LOGGER.debug("Found wildcard pattern in location {}", locationPattern);
                return findPathMatchingResources(locationPattern);
            } else {
                return new Resource[]{this.simpleStorageResourceLoader.getResource(locationPattern)};
            }
        } else {
            return this.resourcePatternResolverDelegate.getResources(locationPattern);
        }
    }

    @SuppressWarnings("PMD.UseStringBufferForStringAppends")
    protected Resource[] findPathMatchingResources(String locationPattern) {
        // Separate the bucket and key patterns as each one uses a different gcs API for resolving.
        String bucketPattern = NameUtils.getBucketNameFromLocation(locationPattern);
        String keyPattern = NameUtils.getObjectNameFromLocation(locationPattern);
        Set<Resource> resources;
        if (this.pathMatcher.isPattern(bucketPattern)) {
            List<String> matchingBuckets = findMatchingBuckets(bucketPattern);
            LOGGER.debug("Found wildcard in bucket name {} buckets found are {}", bucketPattern, matchingBuckets);

            // If the '**' wildcard is used in the bucket name, one have to inspect all
            // objects in the bucket. Therefore the keyPattern is prefixed with '**/' so
            // that the findPathMatchingKeys method knows that it must go through all objects.
            if (bucketPattern.startsWith("**")) {
                keyPattern = "**/" + keyPattern;
            }
            resources = findPathMatchingKeys(keyPattern, matchingBuckets);
            LOGGER.debug("Found resources {} in buckets {}", resources, matchingBuckets);

        } else {
            LOGGER.debug("No wildcard in bucket name {} using single bucket name", bucketPattern);
            resources = findPathMatchingKeys(keyPattern, Collections.singletonList(bucketPattern));
        }

        return resources.toArray(new Resource[resources.size()]);
    }

    private Set<Resource> findPathMatchingKeys(String keyPattern, List<String> matchingBuckets) {
        Set<Resource> resources = new HashSet<>();
        if (this.pathMatcher.isPattern(keyPattern)) {
            for (String bucketName : matchingBuckets) {
                findPathMatchingKeyInBucket(bucketName, resources, null, keyPattern);
            }
        } else {
            for (String matchingBucket : matchingBuckets) {
                Resource resource = this.simpleStorageResourceLoader.getResource(NameUtils.getLocationForBucketAndObject(matchingBucket, keyPattern));
                if (resource.exists()) {
                    resources.add(resource);
                }
            }
        }
        return resources;
    }

    private void findPathMatchingKeyInBucket(String bucketName, Set<Resource> resources, String prefix, String keyPattern) {
        String remainingPatternPart = getRemainingPatternPart(keyPattern, prefix);
        if (remainingPatternPart != null && remainingPatternPart.startsWith("**")) {
            findAllResourcesThatMatches(bucketName, resources, prefix, keyPattern);
        } else {
            findProgressivelyWithPartialMatch(bucketName, resources, prefix, keyPattern);
        }
    }

    private void findAllResourcesThatMatches(String bucketName, Set<Resource> resources, String prefix, String keyPattern) {
        Page<Blob> page = prefix == null ? storage.list(bucketName) : storage.list(bucketName, Storage.BlobListOption.prefix(prefix));
        List<Blob> blobs = new ArrayList<>();
        page.iterateAll().forEach(blobs::add);
        resources.addAll(getResourcesFromObjectSummaries(bucketName, keyPattern, blobs));
    }

    /**
     * Searches for matching keys progressively. This means that instead of retrieving all keys given a prefix, it goes
     * down one level at a time and filters out all non-matching results. This avoids a lot of unused requests results.
     * WARNING: This method does not truncate results. Therefore all matching resources will be returned regardless of
     * the truncation.
     */
    private void findProgressivelyWithPartialMatch(String bucketName, Set<Resource> resources, String prefix, String keyPattern) {
        findAllResourcesThatMatches(bucketName, resources, prefix, keyPattern);
    }

    private String getRemainingPatternPart(String keyPattern, String path) {
        int numberOfSlashes = StringUtils.countOccurrencesOf(path, "/");
        int indexOfNthSlash = getIndexOfNthOccurrence(keyPattern, "/", numberOfSlashes);
        return indexOfNthSlash == -1 ? null : keyPattern.substring(indexOfNthSlash);
    }

    private int getIndexOfNthOccurrence(String str, String sub, int pos) {
        int result = 0;
        String subStr = str;
        for (int i = 0; i < pos; i++) {
            int nthOccurrence = subStr.indexOf(sub);
            if (nthOccurrence == -1) {
                return -1;
            } else {
                result += nthOccurrence + 1;
                subStr = subStr.substring(nthOccurrence + 1);
            }
        }
        return result;
    }

    private Set<Resource> getResourcesFromObjectSummaries(String bucketName, String keyPattern, List<Blob> objectSummaries) {
        return objectSummaries.stream()
                .filter(b -> pathMatcher.match(keyPattern, b.getName()))
                .map(b -> NameUtils.getLocationForBucketAndObject(bucketName, b.getName()))
                .map(simpleStorageResourceLoader::getResource)
                .filter(Resource::exists)
                .collect(toSet());
    }

    private List<String> findMatchingBuckets(String bucketPattern) {
        List<Bucket> buckets = new ArrayList<>();
        Page<Bucket> page = storage.list();
        page.iterateAll().forEach(buckets::add);
        return buckets.stream()
                .map(Bucket::getName)
                .filter(n -> pathMatcher.match(bucketPattern, n))
                .collect(toList());
    }

    @Override
    public Resource getResource(String location) {
        return this.simpleStorageResourceLoader.getResource(location);
    }

    @SuppressWarnings("PMD.UseProperClassLoader")
    @Override
    public ClassLoader getClassLoader() {
        return this.simpleStorageResourceLoader.getClassLoader();
    }
}
